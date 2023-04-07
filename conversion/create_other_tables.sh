#!/bin/bash -i

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists 
    queries
    ,files_datasets
    ,datasets
    ,parameter_definitions
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

create table parameter_definitions
(
    category    text    references parameter_categories(path),
    name        text,
    type        text
        constraint attribute_types check ( 
            type in ('int','double','text','boolean',
                    'int array','double array','text array','boolean array')
            ),
    int_values      bigint[],
    int_min         bigint,
    int_max         bigint,
    double_values   double precision[],
    double_min      double precision,
    double_max      double precision,
    text_values     text[],
    text_pattern    text,
    bollean_value   boolean,
    required        boolean,
    creator             text references users(username),
    created_timestamp   timestamp with time zone        default now(),
    primary key(category, name)
);

    


_EOF_



