#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/attrs_2.csv \
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


_EOF_

preload_json_meta ./data/attrs_2.csv




