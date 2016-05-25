/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Query deparser for cassandra_fdw based on the one from postgres_fdw.
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution, as well as
 * functions to construct the query text to be sent.  We only need deparse
 * logic for node types that we consider safe to send.
 *
 * We schema-qualify all names in this module.
 *
 * Copyright (c) 2014-2016, BigSQL
 * Portions Copyright (c) 2012-2015, PostgreSQL Global Development Group & Others
 *
 * IDENTIFICATION
 *		  cassandra_fdw/deparse.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cstar_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type, or
				 * it has default collation that is not
				 * traceable to a foreign Var */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation is non-default and derives from
				 * something other than a foreign Var */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
} deparse_expr_cxt;

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool
foreign_expr_walker(Node *node,
					foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt);

static bool is_builtin(Oid procid);
/*
 * Functions to construct string representation of a node tree.
 */
static void cassDeparseTargetList(StringInfo buf,
					  PlannerInfo *root,
					  Index rtindex,
					  Relation rel,
					  Bitmapset *attrs_used,
					  List **retrieved_attrs);
static void cassDeparseColumnRef(StringInfo buf, int varno, int varattno,
					 PlannerInfo *root);
static void cassDeparseRelation(StringInfo buf, Relation rel);

/*
 * Functions to construct string representation of a node tree.
 */
static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
static void deparseColumnRef(StringInfo buf, int varno, int varattno,
							 PlannerInfo *root);
static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
static void deparseVar(Var *node, deparse_expr_cxt *context);
static void deparseConst(Const *node, deparse_expr_cxt *context);
static void deparseParam(Param *node, deparse_expr_cxt *context);
static void deparseArrayRef(ArrayRef *node, deparse_expr_cxt *context);
static void deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context);
static void deparseOpExpr(OpExpr *node, deparse_expr_cxt *context);
static void deparseOperatorName(StringInfo buf, Form_pg_operator opform);
static void deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context);
static void deparseScalarArrayOpExpr(ScalarArrayOpExpr *node,
									 deparse_expr_cxt *context);
static void deparseRelabelType(RelabelType *node, deparse_expr_cxt *context);
static void deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context);
static void deparseNullTest(NullTest *node, deparse_expr_cxt *context);
static void deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context);
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod,
							 deparse_expr_cxt *context);
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod,
								   deparse_expr_cxt *context);

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
cassDeparseRelation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *nspname = NULL;
	const char *relname = NULL;
	ListCell   *lc;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "schema_name") == 0)
			nspname = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	if (nspname == NULL)
		nspname = get_namespace_name(RelationGetNamespace(rel));
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s.%s",
					 quote_identifier(nspname), quote_identifier(relname));
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
cassDeparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_relid_attribute_name(rte->relid, varattno);

	appendStringInfoString(buf, quote_identifier(colname));
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for SELECT.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
cassDeparseTargetList(StringInfo buf,
                  PlannerInfo *root,
                  Index rtindex,
                  Relation rel,
                  Bitmapset *attrs_used,
                  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
	                              attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i - 1];

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
		    bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
		                  attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			cassDeparseColumnRef(buf, rtindex, i, root);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first)
		appendStringInfoString(buf, "NULL");
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... FROM tablename".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
void
cassDeparseSelectSql(StringInfo buf,
                 PlannerInfo *root,
                 RelOptInfo *baserel,
                 Bitmapset *attrs_used,
                 List **retrieved_attrs)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	Relation	rel;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");
	cassDeparseTargetList(buf, root, baserel->relid, rel, attrs_used,
	                  retrieved_attrs);

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	cassDeparseRelation(buf, rel);

	elog(DEBUG5, CSTAR_FDW_NAME ": built statement: \"%s\"", buf->data);

	heap_close(rel, NoLock);
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseInsertSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *targetAttrs, bool doNothing)
{
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "INSERT INTO ");
	cassDeparseRelation(buf, rel);

	if (targetAttrs)
	{
		appendStringInfoChar(buf, '(');

		first = true;
		foreach(lc, targetAttrs)
		{
			int			attnum = lfirst_int(lc);

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			cassDeparseColumnRef(buf, rtindex, attnum, root);
		}

		appendStringInfoString(buf, ") VALUES (");

		pindex = 1;
		first = true;
		foreach(lc, targetAttrs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			appendStringInfo(buf, "?");
			pindex++;
		}

		appendStringInfoChar(buf, ')');
	}
	else
		appendStringInfoString(buf, " DEFAULT VALUES");

	if (doNothing)
		appendStringInfoString(buf, " ON CONFLICT DO NOTHING");

	elog(DEBUG5, CSTAR_FDW_NAME ": built statement: \"%s\"", buf->data);
}

/*
 * deparse remote UPDATE statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseUpdateSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List *targetAttrs, const char *primaryKey)
{
	bool		first;
	ListCell   *lc;

	appendStringInfoString(buf, "UPDATE ");
	cassDeparseRelation(buf, rel);
	appendStringInfoString(buf, " SET ");

	first = true;

	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		cassDeparseColumnRef(buf, rtindex, attnum, root);
		appendStringInfoString(buf, " = ?");
	}

	appendStringInfo(buf, " WHERE %s = ?", primaryKey);

	elog(DEBUG5, CSTAR_FDW_NAME ": built statement: \"%s\"", buf->data);
}

/*
 * deparse remote DELETE statement
 *
 * The statement text is appended to buf.
 */
void
cassDeparseDeleteSql(StringInfo buf, PlannerInfo *root,
				 Index rtindex, Relation rel,
				 List **retrieved_attrs,
				 const char *primaryKey)
{
	appendStringInfoString(buf, "DELETE FROM ");
	cassDeparseRelation(buf, rel);
	appendStringInfo(buf, " WHERE %s = ?", primaryKey);

	elog(DEBUG5, CSTAR_FDW_NAME ": built statement: \"%s\"", buf->data);
}

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
cassClassifyConditions(PlannerInfo *root,
					   RelOptInfo *baserel,
					   List *input_conds,
					   List **remote_conds,
					   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (is_cass_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
is_cass_foreign_expr(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/* Expressions examined here should be boolean, ie noncollatable */
	Assert(loc_cxt.collation == InvalidOid);
	Assert(loc_cxt.state == FDW_COLLATE_NONE);

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
	{
		elog(DEBUG3, CSTAR_FDW_NAME
		     ": pushdown prevented because of mutable function(s)");
		return false;
	}

	elog(DEBUG3, CSTAR_FDW_NAME ": pushdown expression checks passed");
	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * There is a note in postgres_fdw that a problem with this is that older
 * versions of remote PostgreSQL servers might not have some of the newer
 * built-in objects on the current system.  This does not quite apply to us
 * for Cassandra in the same manner but there are a few ongoing considerations
 * for the calling code making decisions on top of this function:
 *
 * - If a Cassandra system is older than the version we add decisions atop
 *   this code for, we will not be accurate and can possibly push down clauses
 *   we should not.  However, we intend to only support a specific version
 *   (and up) of Cassandra with an FDW release so this is likely not a major
 *   concern.
 *
 * - The flip side of the last point: if a Cassandra system is newer, we may
 *   not be pushing down everything we can.  There is no direct functional
 *   issue here but our initial narrow coverage of pushdown will probably
 *   continue to be a bigger issue from performance and scalability
 *   perspectives for some time while we work on adding more pushdown.
 *
 * - As the PostgreSQL built-in objects increase, we could consider adding
 *   more pushdown for any corresponding Cassandra objects that exist and that
 *   the pushdown makes sense for.
 */
static bool
is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse.
 */
static bool
foreign_expr_walker(Node *node,
                    foreign_glob_cxt *glob_cxt,
                    foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				elog(DEBUG4, CSTAR_FDW_NAME ": pushdown check for T_BoolExpr");

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_Var:
		{
			Var		   *var = (Var *) node;
			elog(DEBUG4, CSTAR_FDW_NAME ": pushdown check for T_Var");

			/*
			 * If the Var is from the foreign table, we consider its
			 * collation (if any) safe to use.  If it is from another
			 * table, we treat its collation the same way as we would a
			 * Param's collation, ie it's not safe for it to have a
			 * non-default collation.
			 */
			if (var->varno == glob_cxt->foreignrel->relid &&
			    var->varlevelsup == 0)
			{
				/* Var belongs to foreign table */

				/*
				 * System columns other than ctid should not be sent to
				 * the remote, since we don't make any effort to ensure
				 * that local and remote values match (tableoid, in
				 * particular, almost certainly doesn't match).
				 */
				if (var->varattno < 0 &&
				    var->varattno != SelfItemPointerAttributeNumber)
					return false;

				/* Else check the collation */
				collation = var->varcollid;
				state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
			}
			else
			{
				/* Var belongs to some other table */
				collation = var->varcollid;
				if (collation == InvalidOid ||
				    collation == DEFAULT_COLLATION_OID)
				{
					/*
					 * It's noncollatable, or it's safe to combine with a
					 * collatable foreign Var, so set state to NONE.
					 */
					state = FDW_COLLATE_NONE;
				}
				else
				{
					/*
					 * Do not fail right away, since the Var might appear
					 * in a collation-insensitive context.
					 */
					state = FDW_COLLATE_UNSAFE;
				}
			}
		}
		break;
		case T_Const:
		{
			Const	   *c = (Const *) node;
			elog(DEBUG4, CSTAR_FDW_NAME ": pushdown check for T_Const");

			/*
			 * If the constant has nondefault collation, either it's of a
			 * non-builtin type, or it reflects folding of a CollateExpr.
			 * It's unsafe to send to the remote unless it's used in a
			 * non-collation-sensitive context.
			 */
			collation = c->constcollid;
			if (collation == InvalidOid ||
			    collation == DEFAULT_COLLATION_OID)
				state = FDW_COLLATE_NONE;
			else
				state = FDW_COLLATE_UNSAFE;
		}
		break;
		case T_Param:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_Param");
			return false;
		}
		case T_ArrayRef:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_ArrayRef");
			return false;
		}
		case T_FuncExpr:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_FuncExpr");
			return false;
		}
		case T_OpExpr:
		{
			OpExpr	   *oe = (OpExpr *) node;
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown check for T_OpExpr");

			/*
			 * Similarly, only built-in operators can be sent to remote.
			 * (If the operator is, surely its underlying function is
			 * too.)
			 */
			if (!is_builtin(oe->opno))
				return false;

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) oe->args,
			                         glob_cxt, &inner_cxt))
				return false;

			/*
			 * If operator's input collation is not derived from a foreign
			 * Var, it can't be sent to remote.
			 */
			if (oe->inputcollid == InvalidOid)
				/* OK, inputs are all noncollatable */ ;
			else if (inner_cxt.state != FDW_COLLATE_SAFE ||
			         oe->inputcollid != inner_cxt.collation)
				return false;

			/* Result-collation handling is same as for functions */
			collation = oe->opcollid;
			if (collation == InvalidOid)
				state = FDW_COLLATE_NONE;
			else if (inner_cxt.state == FDW_COLLATE_SAFE &&
			         collation == inner_cxt.collation)
				state = FDW_COLLATE_SAFE;
			else if (collation == DEFAULT_COLLATION_OID)
				state = FDW_COLLATE_NONE;
			else
				state = FDW_COLLATE_UNSAFE;
		}
		break;
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_DistinctExpr");
			return false;
		}
		case T_ScalarArrayOpExpr:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_ScalarArrayOpExpr");
			return false;
		}
		case T_RelabelType:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_RelabelType");
			return false;
		}
		case T_NullTest:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_NullTest");
			return false;
		}
		case T_ArrayExpr:
		{
			elog(DEBUG4,
			     CSTAR_FDW_NAME ": pushdown not supported for T_ArrayExpr");
			return false;
		}
		case T_List:
		{
			List	   *l = (List *) node;
			ListCell   *lc;
			elog(DEBUG4, CSTAR_FDW_NAME ": pushdown check for T_List");

			/*
			 * Recurse to component subexpressions.
			 */
			foreach(lc, l)
			{
				if (!foreign_expr_walker((Node *) lfirst(lc),
				                         glob_cxt, &inner_cxt))
					return false;
			}

			/*
			 * When processing a list, collation state just bubbles up
			 * from the list elements.
			 */
			collation = inner_cxt.collation;
			state = inner_cxt.state;

			/* Don't apply exprType() to the list. */
			check_type = false;
		}
		break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			elog(DEBUG4, CSTAR_FDW_NAME
			     ": pushing down not supported for <Unknown>");
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !is_builtin(exprType(node)))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
		case FDW_COLLATE_NONE:
			/* Nothing + nothing is still nothing */
			break;
		case FDW_COLLATE_SAFE:
			if (collation != outer_cxt->collation)
			{
				/*
				 * Non-default collation always beats default.
				 */
				if (outer_cxt->collation == DEFAULT_COLLATION_OID)
				{
					/* Override previous parent state */
					outer_cxt->collation = collation;
				}
				else if (collation != DEFAULT_COLLATION_OID)
				{
					/*
					 * Conflict; show state as indeterminate.  We don't
					 * want to "return false" right away, since parent
					 * node might not care about collation.
					 */
					outer_cxt->state = FDW_COLLATE_UNSAFE;
				}
			}
			break;
		case FDW_COLLATE_UNSAFE:
			/* We're still conflicted ... */
			break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * Deparse WHERE clauses in given list of RestrictInfos and append them to buf.
 *
 * baserel is the foreign table we're planning for.
 *
 * If no WHERE clause already exists in the buffer, is_first should be true.
 *
 * If params is not NULL, it receives a list of Params and other-relation Vars
 * used in the clauses; these values must be transmitted to the remote server
 * as parameter values.
 *
 * If params is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 */
void
appendWhereClause(StringInfo buf,
				  PlannerInfo *root,
				  RelOptInfo *baserel,
				  List *exprs,
				  bool is_first,
				  List **params)
{
	deparse_expr_cxt context;
	int			nestlevel;
	ListCell   *lc;

	if (params)
		*params = NIL;			/* initialize result list to empty */

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = baserel;
	context.buf = buf;
	context.params_list = params;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
			appendStringInfoString(buf, " WHERE ");
		else
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(ri->clause, &context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

	if (exprs)
	{
		appendStringInfoString(buf, " ALLOW FILTERING");
		elog(DEBUG5, CSTAR_FDW_NAME ": updated statement for pushdown: \"%s\"",
		     buf->data);
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
deparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_relid_attribute_name(rte->relid, varattno);

	appendStringInfoString(buf, quote_identifier(colname));
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
void
deparseStringLiteral(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
	case T_Var:
		deparseVar((Var *) node, context);
		break;
	case T_Const:
		deparseConst((Const *) node, context);
		break;
	case T_Param:
		deparseParam((Param *) node, context);
		break;
	case T_ArrayRef:
		deparseArrayRef((ArrayRef *) node, context);
		break;
	case T_FuncExpr:
		deparseFuncExpr((FuncExpr *) node, context);
		break;
	case T_OpExpr:
		deparseOpExpr((OpExpr *) node, context);
		break;
	case T_DistinctExpr:
		deparseDistinctExpr((DistinctExpr *) node, context);
		break;
	case T_ScalarArrayOpExpr:
		deparseScalarArrayOpExpr((ScalarArrayOpExpr *) node, context);
		break;
	case T_RelabelType:
		deparseRelabelType((RelabelType *) node, context);
		break;
	case T_BoolExpr:
		deparseBoolExpr((BoolExpr *) node, context);
		break;
	case T_NullTest:
		deparseNullTest((NullTest *) node, context);
		break;
	case T_ArrayExpr:
		deparseArrayExpr((ArrayExpr *) node, context);
		break;
	default:
		elog(ERROR, "unsupported expression type for deparse: %d",
		     (int) nodeTag(node));
		break;
	}
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
deparseVar(Var *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	if (node->varno == context->foreignrel->relid &&
	    node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		deparseColumnRef(buf, node->varno, node->varattno, context->root);
	}
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = 0;
			ListCell   *lc;

			/* find its index in params_list */
			foreach(lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}

			printRemoteParam(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			printRemotePlaceholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 */
static void
deparseConst(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		isfloat = false;
	bool		needlabel;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		appendStringInfo(buf, "::%s",
		                 format_type_with_typemod(node->consttype,
		                                          node->consttypmod));
		return;
	}

	getTypeOutputInfo(node->consttype,
	                  &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	switch (node->consttype)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	{
		/*
		 * No need to quote unless it's a special value such as 'NaN'.
		 * See comments in get_const_expr().
		 */
		if (strspn(extval, "0123456789+-eE.") == strlen(extval))
		{
			if (extval[0] == '+' || extval[0] == '-')
				appendStringInfo(buf, "(%s)", extval);
			else
				appendStringInfoString(buf, extval);
			if (strcspn(extval, "eE.") != strlen(extval))
				isfloat = true; /* it looks like a float */
		}
		else
			appendStringInfo(buf, "'%s'", extval);
	}
	break;
	case BITOID:
	case VARBITOID:
		appendStringInfo(buf, "B'%s'", extval);
		break;
	case BOOLOID:
		if (strcmp(extval, "t") == 0)
			appendStringInfoString(buf, "true");
		else
			appendStringInfoString(buf, "false");
		break;
	default:
		deparseStringLiteral(buf, extval);
		break;
	}

	/*
	 * Append ::typename unless the constant will be implicitly typed as the
	 * right type when it is read in.
	 *
	 * XXX this code has to be kept in sync with the behavior of the parser,
	 * especially make_const.
	 */
	switch (node->consttype)
	{
	case BOOLOID:
	case INT4OID:
	case UNKNOWNOID:
		needlabel = false;
		break;
	case NUMERICOID:
		needlabel = !isfloat || (node->consttypmod >= 0);
		break;
	default:
		needlabel = true;
		break;
	}
	if (needlabel)
		appendStringInfo(buf, "::%s",
		                 format_type_with_typemod(node->consttype,
		                                          node->consttypmod));
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void
deparseParam(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int			pindex = 0;
		ListCell   *lc;

		/* find its index in params_list */
		foreach(lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
				break;
		}
		if (lc == NULL)
		{
			/* not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		printRemoteParam(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		printRemotePlaceholder(node->paramtype, node->paramtypmod, context);
	}
}

/*
 * Deparse an array subscript expression.
 */
static void
deparseArrayRef(ArrayRef *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lowlist_item;
	ListCell   *uplist_item;

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/*
	 * Deparse referenced array expression first.  If that expression includes
	 * a cast, we have to parenthesize to prevent the array subscript from
	 * being taken as typename decoration.  We can avoid that in the typical
	 * case of subscripting a Var, but otherwise do it.
	 */
	if (IsA(node->refexpr, Var))
		deparseExpr(node->refexpr, context);
	else
	{
		appendStringInfoChar(buf, '(');
		deparseExpr(node->refexpr, context);
		appendStringInfoChar(buf, ')');
	}

	/* Deparse subscript expressions. */
	lowlist_item = list_head(node->reflowerindexpr);	/* could be NULL */
	foreach(uplist_item, node->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');
		if (lowlist_item)
		{
			deparseExpr(lfirst(lowlist_item), context);
			appendStringInfoChar(buf, ':');
			lowlist_item = lnext(lowlist_item);
		}
		deparseExpr(lfirst(uplist_item), context);
		appendStringInfoChar(buf, ']');
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a function call.
 */
static void
deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	bool		use_variadic;
	bool		first;
	ListCell   *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * If the function call came from a cast, then show the first argument
	 * plus an explicit cast operation.
	 */
	if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		Oid			rettype = node->funcresulttype;
		int32		coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

		deparseExpr((Expr *) linitial(node->args), context);
		appendStringInfo(buf, "::%s",
		                 format_type_with_typemod(rettype, coercedTypmod));
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->funcvariadic;

	/* Print schema name only if it's not pg_catalog */
	if (procform->pronamespace != PG_CATALOG_NAMESPACE)
	{
		const char *schemaname;

		schemaname = get_namespace_name(procform->pronamespace);
		appendStringInfo(buf, "%s.", quote_identifier(schemaname));
	}

	/* Deparse the function name ... */
	proname = NameStr(procform->proname);
	appendStringInfo(buf, "%s(", quote_identifier(proname));
	/* ... and all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		if (use_variadic && lnext(arg) == NULL)
			appendStringInfoString(buf, "VARIADIC ");
		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(proctup);
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
deparseOpExpr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
	       (oprkind == 'l' && list_length(node->args) == 1) ||
	       (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	deparseOperatorName(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
deparseOperatorName(StringInfo buf, Form_pg_operator opform)
{
	char	   *opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)",
		                 quote_identifier(opnspname), opname);
	}
	else
	{
		/* Just print operator name. */
		appendStringInfoString(buf, opname);
	}
}

/*
 * Deparse IS DISTINCT FROM.
 */
static void
deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	deparseExpr(linitial(node->args), context);
	appendStringInfoString(buf, " IS DISTINCT FROM ");
	deparseExpr(lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	Expr	   *arg1;
	Expr	   *arg2;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	arg1 = linitial(node->args);
	deparseExpr(arg1, context);
	appendStringInfoChar(buf, ' ');

	/* Deparse operator name plus decoration. */
	deparseOperatorName(buf, form);
	appendStringInfo(buf, " %s (", node->useOr ? "ANY" : "ALL");

	/* Deparse right operand. */
	arg2 = lsecond(node->args);
	deparseExpr(arg2, context);

	appendStringInfoChar(buf, ')');

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
deparseRelabelType(RelabelType *node, deparse_expr_cxt *context)
{
	deparseExpr(node->arg, context);
	if (node->relabelformat != COERCE_IMPLICIT_CAST)
		appendStringInfo(context->buf, "::%s",
		                 format_type_with_typemod(node->resulttype,
		                                          node->resulttypmod));
}

/*
 * Deparse a BoolExpr node.
 */
static void
deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
	case AND_EXPR:
		op = "AND";
		break;
	case OR_EXPR:
		op = "OR";
		break;
	case NOT_EXPR:
		appendStringInfoString(buf, "(NOT ");
		deparseExpr(linitial(node->args), context);
		appendStringInfoChar(buf, ')');
		return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
deparseNullTest(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	deparseExpr(node->arg, context);
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "ARRAY[");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ']');

	/* If the array is empty, we need an explicit cast to the array type. */
	if (node->elements == NIL)
		appendStringInfo(buf, "::%s",
		                 format_type_with_typemod(node->array_typeid, -1));
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod,
                 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	char	   *ptypename = format_type_with_typemod(paramtype, paramtypmod);

	appendStringInfo(buf, "$%d::%s", paramindex, ptypename);
}

/*
 * Print the representation of a placeholder for a parameter that will be
 * sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query.
 * We don't have the actual value of the runtime parameter yet, and we don't
 * want the remote planner to generate a plan that depends on such a value
 * anyway.  Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of Postgres, the planner will see that as an unknown
 * constant value, which is what we want.  This might need adjustment if we
 * ever make the planner flatten scalar subqueries.  Note: the reason for the
 * apparently useless outer cast is to ensure that the representation as a
 * whole will be parsed as an a_expr and not a select_with_parens; the latter
 * would do the wrong thing in the context "x = ANY(...)".
 */
static void
printRemotePlaceholder(Oid paramtype, int32 paramtypmod,
                       deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	char	   *ptypename = format_type_with_typemod(paramtype, paramtypmod);

	appendStringInfo(buf, "((SELECT null::%s)::%s)", ptypename, ptypename);
}
