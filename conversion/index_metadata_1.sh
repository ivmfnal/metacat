#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop index if exists files_meta_index;
create index files_meta_index on files using gin (metadata);

_EOF_
