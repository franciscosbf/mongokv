EXTENSION=mongokv
MODULE_big=mongokv
OBJS=mongokv.o
DATA=mongokv--1.0.0.sql
PG_CPPFLAGS=$(shell pkg-config --cflags libmongoc-1.0)
SHLIB_LINK=$(shell pkg-config --libs libmongoc-1.0)
PG_CONFIG=pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
