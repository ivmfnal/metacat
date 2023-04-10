#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q > ./data/event_numbers.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
    select f.file_id, '${core_category}.events', to_json(e.event_numbers)
        		from active_files f, dune.events e
        		where f.file_id = e.file_id
        
) to stdout;

_EOF_

preload_json_meta ./data/event_numbers.csv


