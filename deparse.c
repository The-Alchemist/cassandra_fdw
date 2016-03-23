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
 *		  contrib/cassandra_fdw/deparse.c
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

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);

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

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
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

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
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

	elog(DEBUG1, CSTAR_FDW_NAME ": built the statement: %s", buf->data);
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
		return false;

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
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
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
