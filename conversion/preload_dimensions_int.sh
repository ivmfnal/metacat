#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/dims_int.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

-- int attrs
copy (
	select f.file_id, pc.param_category || '.' || pt.param_type, param_value
                from active_files f
                inner join num_data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
        where dt.data_type_id = 5
                    and param_value is not null
        odrer by f.file_id
) to stdout;


_EOF_

preload_json_meta ./data/dims_int.csv

