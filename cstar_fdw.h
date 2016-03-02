
#ifndef CASSANDRA_FDW_H_
#define CASSANDRA_FDW_H_

#include <cassandra.h>

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"

/* in cstar_connect.c */
extern CassSession *pgcass_GetConnection(ForeignServer *server, UserMapping *user,
			  bool will_prep_stmt);
extern void pgcass_ReleaseConnection(CassSession *session);

extern void pgcass_report_error(int elevel, CassFuture* result_future,
				bool clear, const char *sql);

#endif /* CASSANDRA_FDW_H_ */
