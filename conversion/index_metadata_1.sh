#!/bin/sh

source ./config.sh

exec $OUT_DB_PSQL << _EOF_

create index files_meta_index on files using gin (metadata);

_EOF_
