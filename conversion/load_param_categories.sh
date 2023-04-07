#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/param_categories.csv \
	<< _EOF_

copy (
    select pc.param_category, 'admin', 'admin', '{}'
        from param_categories pc
) to stdout;
_EOF_

$OUT_DB_PSQL -q <<_EOF_

drop table if exists parameter_categories cascade;
create table parameter_categories
(
    path        text    primary key,
    
	owner_user          text,
	owner_role          text,
    
    check ( (owner_user is null ) != (owner_role is null) ),
    
    restricted  boolean default 'false',
    description         text,
    creator             text references users(username),
    created_timestamp   timestamp with time zone     default now(),
    definitions         jsonb
);

\echo imporing parameter categories ...

\copy parameter_categories(path, owner_user, creator, definitions) from './data/param_categories.csv';

insert into parameter_categories(path, owner_user, creator, definitions)
    values('${core_category}', 'admin', 'admin', '{}'::jsonb);
    
_EOF_




