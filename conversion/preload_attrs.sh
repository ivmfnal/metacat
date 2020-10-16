#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/attrs.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy ( 
    select df.file_id, 'SAM.event_count', df.event_count, null, null, null, null
                from active_files df
                where df.event_count is not null
) to stdout;

copy ( 
    select df.file_id, 'SAM.first_event_number', df.first_event_number, null, null, null, null
                from active_files df
                where df.first_event_number is not null
) to stdout;

copy ( 
    select df.file_id, 'SAM.last_event_number', df.last_event_number, null, null, null, null
                from active_files df
                where df.last_event_number is not null
) to stdout;

copy (
   select df.file_id, 'SAM.start_time', null, extract(epoch from df.start_time), null, null, null
                from active_files df
                where df.start_time is not null
) to stdout;

copy (
   select df.file_id, 'SAM.end_time', null, extract(epoch from df.end_time), null, null, null
                from active_files df
                where df.end_time is not null
) to stdout;

copy (
	select f.file_id, pc.param_category || '.' || pt.param_type, param_value, null, null, null, null
                from active_files f
                inner join num_data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type_id = 5
                    and param_value is not null

) to stdout;


-- float attrs
copy (
	select f.file_id, pc.param_category || '.' || pt.param_type, null, param_value, null, null, null
                from active_files f
                inner join num_data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type_id = 6
                    and param_value is not null
) to stdout;

-- string attrs
copy (
   select f.file_id, pc.param_category || '.' || pt.param_type, null, null, pv.param_value, null, null
                from active_files f
                inner join data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_values pv on pv.param_value_id = dfv.param_value_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type = 'string'
                    and pv.param_value is not null
) to stdout;


_EOF_

preload_meta ./data/app_families.csv



