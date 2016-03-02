
MODULE_big = cstar_fdw
OBJS = cstar_fdw.o cstar_connect.o

SHLIB_LINK = -lcassandra

EXTENSION = cstar_fdw
DATA = cstar_fdw--2.2.sql

REGRESS = cstar_fdw

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
