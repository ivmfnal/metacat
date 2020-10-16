#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/file_formats.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (

	select f.file_id, '.file_format', null, null, ff.file_format, null, null
		from active_files f, file_formats ff
		where f.file_format_id = ff.file_format_id
) to stdout;



_EOF_

preload_meta ./data/file_formats.csv
