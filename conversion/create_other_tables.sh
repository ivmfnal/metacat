#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists 
    queries
    ,files_datasets
    ,datasets
    ,namespaces
    cascade
;

--drop table if exists authenticators;
--create table authenticators
--(
--    username    text    references users(username) on delete cascade,
--    type        text
--        constraint authenticator_types check ( 
--            type in ('x509','password','ssh')
--            ),
--    secrets      text[],
--    primary key(username, type)
--);

create table namespaces
(
    name                text    primary key,
    check( name != ''),

    description         text,

    owner_user          text,
    owner_role          text,
    check ( (owner_user is null ) != (owner_role is null) ),

    creator        text,
    created_timestamp   timestamp with time zone        default now()
);

insert into namespaces(name, owner_role, creator)
(
    select distinct namespace, 'admin_role', 'admin' from files
);

insert into namespaces(name, owner_role, creator) values('dune', 'admin_role', 'admin');

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



