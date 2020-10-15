source ./config.sh

$IN_DB_PSQL -q \
	> ./data/detector_type_lists.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;


create temp view string_attrs as
    select f.file_id as file_id, pc.param_category || '.' || pt.param_type as name, pv.param_value as value
                                from active_files f
                                inner join data_files_param_values dfv on f.file_id = dfv.file_id
                                inner join param_values pv on pv.param_value_id = dfv.param_value_id
                                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                                inner join data_types dt on dt.data_type_id = pt.data_type_id
                                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                                where dt.data_type = 'string'
;




copy ( 
    select file_id, regexp_split_to_array(value, ':')
        from string_attrs
        where name = 'lbne_data.detector_type'
    ) to stdout;
_EOF_