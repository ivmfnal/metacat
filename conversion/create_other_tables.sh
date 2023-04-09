#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists queries cascade;

create table queries
(
    namespace       text references namespaces(name),
    name            text,
    parameters      text[],
    source      text,
    primary key(namespace, name),
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now()
);

_EOF_



