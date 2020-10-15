#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/file_content_status.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
    select f.file_id, 'SAM.file_content_status', fcs.file_content_status
        		from active_files f, file_content_statuses fcs
        		where f.file_content_status_id = fcs.file_content_status_id
        
) to stdout;



_EOF_

preload_meta ./data/file_content_status.csv text
