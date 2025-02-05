# need move to contrib/primary_monitor/Makefile

MODULE_big = primary_monitor
OBJS = \
	primary_monitor.o \

EXTENSION = primary_monitor
PGFILEDESC = "primary monitor - autofailover extension"

REGRESS = primary_monitor

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/primary_monitor
top_builddir = /home/nelta/prog/postgres
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
