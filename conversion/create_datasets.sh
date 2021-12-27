#!/bin/sh

source ./config.sh

$OUT_DB_PSQL << _EOF_

drop table if exists 
    files_datasets, datasets, datasets_parent_child
    cascade
;

create table datasets
(
    namespace           text,
    name                text,

    primary key (namespace, name),

    frozen		boolean default 'false',
    monotonic		boolean default 'false',
    metadata    jsonb   default '{}',
    required_metadata   text[],
    creator        text,
    created_timestamp   timestamp with time zone     default now(),
    expiration          timestamp with time zone,
    description         text,
    file_metadata_requirements  jsonb   default '{}'::jsonb
);

create index dataset_specs on datasets((namespace || ':' || name));

insert into datasets(namespace, name, creator, description)
	values('dune','all','admin','All files imported during conversion from SAM');
    

create table datasets_parent_child
(
    parent_namespace text,
    parent_name text,
    child_namespace text,
    child_name text,
    foreign key (parent_namespace, parent_name) references datasets(namespace, name),
    foreign key (child_namespace, child_name) references datasets(namespace, name),
    primary key (parent_namespace, parent_name, child_namespace, child_name)
);

create index datasets_pc_parent_specs on datasets_parent_child((parent_namespace || ':' || parent_name));
create index datasets_pc_child_specs on datasets_parent_child((child_namespace || ':' || child_name));
create index datasets_pc_child on datasets_parent_child(child_namespace, child_name);

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

