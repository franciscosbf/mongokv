EXTENSION=mongokv
MODULE_big=mongokv
OBJS=mongokv.o
DATA=mongokv--1.0.0.sql

MKV_EXTERNAL_LIBS=glib-2.0 libmongoc-1.0

MKV_CFLAGS=$(shell pkg-config --cflags $(MKV_EXTERNAL_LIBS))
MKV_LIBS=$(shell pkg-config --libs $(MKV_EXTERNAL_LIBS))

MKV_WARN_SUPRESS=-Wno-declaration-after-statement

PG_CPPFLAGS=$(MKV_CFLAGS) $(MKV_WARN_SUPRESS)
SHLIB_LINK=$(MKV_LIBS)

PG_CONFIG=pg_config
PGXS:=$(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
