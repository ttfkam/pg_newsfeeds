EXTENSION = newsfeeds          # the extensions name
DATA = $(wildcard *--*.sql)    # script files to install

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
