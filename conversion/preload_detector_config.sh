#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/detector_config_json.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

create temp view detector_configs as
    select f.file_id as file_id, pc.param_category as category, pt.param_type as name, pv.param_value as value
                                from active_files f
                                inner join data_files_param_values dfv on f.file_id = dfv.file_id
                                inner join param_values pv on pv.param_value_id = dfv.param_value_id
                                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                                inner join data_types dt on dt.data_type_id = pt.data_type_id
                                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                                where dt.data_type = 'string'
                                
                                        and pc.param_category='DUNE_data' and pt.param_type = 'detector_config'
                                    
;

copy ( 
    select file_id, 'DUNE_data.detector_config.object',
        case 
            when substr(value, 1, 1) != '{' then array_to_json(regexp_split_to_array(value, ':'))
            else value::json
        end
        from detector_configs
    ) 
to stdout;
_EOF_

preload_json_meta ./data/detector_config_json.csv




