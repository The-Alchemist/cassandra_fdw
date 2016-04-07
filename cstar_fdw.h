#ifndef CASSANDRA_FDW_H_
#define CASSANDRA_FDW_H_

#include <cassandra.h>

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"

/* User-visible name for logging and reporting purposes */
#define CSTAR_FDW_NAME				"cassandra_fdw"
#define MSECS_PER_SEC				1000
#define LITERAL_UTC				"UTC"

/* in cstar_connect.c */
extern CassSession *pgcass_GetConnection(ForeignServer *server, UserMapping *user,
			  bool will_prep_stmt);
extern void pgcass_ReleaseConnection(CassSession *session);

extern void pgcass_report_error(int elevel, CassFuture* result_future,
				bool clear, const char *sql);

/* in cstar_fdw.c */
extern int	set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);

/* in deparse.c */
extern void
cassDeparseSelectSql(StringInfo buf,
					 PlannerInfo *root,
					 RelOptInfo *baserel,
					 Bitmapset *attrs_used,
					 List **retrieved_attrs);
extern void
cassDeparseInsertSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List *targetAttrs, bool doNothing);

extern void
cassDeparseUpdateSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List *targetAttrs, const char *primaryKey);

extern void
cassDeparseDeleteSql(StringInfo buf, PlannerInfo *root,
					 Index rtindex, Relation rel,
					 List **retrieved_attrs,
					 const char *primaryKey);

extern bool
is_cass_foreign_expr(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Expr *expr);

extern void
cassClassifyConditions(PlannerInfo *root,
					   RelOptInfo *baserel,
					   List *input_conds,
					   List **remote_conds,
					   List **local_conds);

extern void
appendWhereClause(StringInfo buf,
				  PlannerInfo *root,
				  RelOptInfo *baserel,
				  List *exprs,
				  bool is_first,
				  List **params);

extern void deparseStringLiteral(StringInfo buf, const char *val);
#endif /* CASSANDRA_FDW_H_ */
