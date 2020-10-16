#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/data_streams.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
    select f.file_id, 'SAM.data_stream', null, null, ds.datastream_name, null, null
        		from active_files f, datastreams ds
        		where f.stream_id = ds.stream_id
) to stdout;



_EOF_

preload_meta ./data/data_streams.csv
