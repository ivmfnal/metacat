#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/attrs.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy ( 
    select df.file_id, '${core_category}.event_count', df.event_count
                from active_files df
                where df.event_count is not null
) to stdout;

copy ( 
    select df.file_id, '${core_category}.first_event_number', df.first_event_number
                from active_files df
                where df.first_event_number is not null
) to stdout;

copy ( 
    select df.file_id, '${core_category}.last_event_number', df.last_event_number
                from active_files df
                where df.last_event_number is not null
) to stdout;

copy (
   select df.file_id, '${core_category}.start_time', extract(epoch from df.start_time)
                from active_files df
                where df.start_time is not null
) to stdout;

copy (
   select df.file_id, '${core_category}.start_time_utc_text', '"' || (df.start_time at time zone 'utc')::text || '"'
                from active_files df
                where df.start_time is not null
) to stdout;

copy (
   select df.file_id, '${core_category}.end_time', extract(epoch from df.end_time)
                from active_files df
                where df.end_time is not null
) to stdout;

copy (
   select df.file_id, '${core_category}.end_time_utc_text', '"' || (df.end_time at time zone 'utc')::text || '"'
                from active_files df
                where df.end_time is not null
) to stdout;

copy (
   select df.file_id, '${core_category}.process_id', df.process_id
                from active_files df
                where df.process_id is not null
) to stdout;

copy (
    select df.file_id, '${origin_category}.worker_id', '"' || df.process_id || '"'      -- convert process id to string
        from active_files df
	where df.process_id is not null
) to stdout;


_EOF_

preload_json_meta ./data/attrs.csv

$IN_DB_PSQL -q \
	> ./data/param_categories.csv \
	<< _EOF_

copy (
    select pc.param_category, 'admin', 'admin', '{}'
        from param_categories pc
) to stdout;
_EOF_

$OUT_DB_PSQL <<_EOF_

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




