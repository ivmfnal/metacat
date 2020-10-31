#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/file_types.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
	select f.file_id, '${core_category}.file_type', to_json(ft.file_type_desc)
		from active_files f, file_types ft
		where f.file_type_id = ft.file_type_id
) to stdout;



_EOF_

preload_json_meta ./data/file_types.csv