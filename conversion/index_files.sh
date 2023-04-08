#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

\echo ... building files namespace:name index ...
create unique index files_names on files(namespace, name);
create index files_name on files(name);
create index files_size on files(size);

\echo ... indexing file attributes ...
create index files_creator on files(creator);
create index files_created_timestamp on files(created_timestamp);
create index files_size on files(size);

_EOF_
