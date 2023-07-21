#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop index if exists files_names, files_name, files_created_timestamp;

\echo ... building files namespace:name index ...
create unique index files_names on files(namespace, name) include (id);
create index files_name on files(name) include (namespace, id);
create index files_created_timestamp on files(created_timestamp);

_EOF_
