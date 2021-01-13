#!/bin/sh

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists 
    files_datasets
    ,datasets
    cascade
;

create table datasets
(
    namespace           text,
    name                text,

    primary key (namespace, name),

    parent_namespace    text,
    parent_name         text,

    foreign key (parent_namespace, parent_name) references datasets(namespace, name),

    frozen		boolean default 'false',
    monotonic		boolean default 'false',
    metadata    jsonb   default '{}',
    required_metadata   text[],
    creator        text,
    created_timestamp   timestamp with time zone     default now(),
    expiration          timestamp with time zone,
    description         text,
    file_metadata_requirements  jsonb
);

insert into datasets(namespace, name, creator, description)
	values('dune','all','admin','All files imported during conversion from SAM');

create table files_datasets
(
    file_id                 text,
    dataset_namespace       text,
    dataset_name            text
);       

\echo Populating dataset "dune:all" ...

insert into files_datasets(file_id, dataset_namespace, dataset_name)
(
	select f.id, 'dune','all'
		from files f
);

