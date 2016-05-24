
MODULE_big = cassandra_fdw
OBJS = cstar_fdw.o cstar_connect.o deparse.o

SHLIB_LINK = -lcassandra

EXTENSION = cassandra_fdw
DATA = cassandra_fdw--3.0.0.sql

REGRESS = cassandra_fdw

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cstar_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
