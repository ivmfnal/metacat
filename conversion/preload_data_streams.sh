#!/bin/bash -i

source ./config.sh

$IN_DB_PSQL -q > ./data/data_streams.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
    select f.file_id, '${core_category}.data_stream', to_json(ds.datastream_name)
        		from active_files f, datastreams ds
        		where f.stream_id = ds.stream_id
) to stdout;



_EOF_

preload_json_meta ./data/data_streams.csv
