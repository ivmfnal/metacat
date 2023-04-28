#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists namespaces cascade;

create table namespaces
(
    name                text    primary key,
    check( name != ''),

    description         text,

    owner_user          text,
    owner_role          text,
    check ( (owner_user is null ) != (owner_role is null) ),

    creator        text,
    created_timestamp   timestamp with time zone        default now(),
    file_count  bigint  default 0
);

insert into namespaces(name, owner_role, creator) values('${default_namespace}', 'admin_role', 'admin')
on conflict(name) do nothing;

insert into namespaces(name, owner_role, creator)
(
    select distinct namespace, 'admin_role', 'admin' from files
)
on conflict(name) do nothing;

_EOF_



