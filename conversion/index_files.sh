#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop index if exists files_names, files_name, files_size,
    files_creator, files_created_timestamp, files_size;

\echo ... building files namespace:name index ...
create unique index files_names on files(namespace, name);
create index files_name on files(name);
create index files_size on files(size);


create index files_creator on files(creator);
create index files_created_timestamp on files(created_timestamp);

_EOF_
