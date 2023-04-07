#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/attrs_1.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

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

preload_json_meta ./data/attrs_1.csv




