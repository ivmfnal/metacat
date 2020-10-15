#!/bin/sh

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists 
    queries
    ,files_datasets
    ,datasets
    ,authenticators
    ,parameter_definitions
    ,parameter_categories
    ,namespaces
    ,roles
;

create table roles
(
    name        text    primary key,
    description text,
    users       text[]  default '{}'::text[]
);

create table authenticators
(
    username    text    references users(username) on delete cascade,
    type        text
        constraint authenticator_types check ( 
            type in ('x509','password','ssh')
            ),
    secrets      text[],
    primary key(username, type)
);

create table namespaces
(
    name                text        primary key,
    owner               text        references  roles(name),
    creator        text references users(username),
    created_timestamp   timestamp with time zone        default now()
);

insert into roles(name, description, users) values ('admin','Admin user','{"admin"}');

insert into namespaces(name, owner, creator) values ('dune','admin','admin');

create table datasets
(
    namespace           text references namespaces(name),
    name                text,
    parent_namespace    text,
    parent_name         text,
    frozen		boolean default 'false',
    monotonic		boolean default 'false',
    primary key (namespace, name),
    foreign key (parent_namespace, parent_name) references datasets(namespace, name),
    metadata    jsonb,
    required_metadata   text[],
    creator        text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    expiration          timestamp with time zone,
    description         text
);

insert into datasets(namespace, name, creator, description)
	values('dune','all','admin','All files imported during conversion from SAM');

create index datasets_meta_index on datasets using gin (metadata);

create table files_datasets
(
    file_id                 text,
    dataset_namespace       text,
    dataset_name            text
);       

\echo Populating dataset "all" ...

insert into files_datasets(file_id, dataset_namespace, dataset_name)
(
	select f.id, 'dune','all'
		from files f
);

alter table files_datasets add primary key (dataset_namespace, dataset_name, file_id);
create index files_datasets_file_id on files_datasets(file_id);

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

create table parameter_categories
(
    path        text    primary key,
    owner       text    references  roles(name),
    restricted  boolean default 'false',
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    definitions         jsonb
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

create table authenticators
(
    username    text    references users(username) on delete cascade,
    type        text
        constraint authenticator_types check ( 
            type in ('x509','password','ssh')
            ),
    secrets      text[],
    primary key(username, type)
);


    


_EOF_



