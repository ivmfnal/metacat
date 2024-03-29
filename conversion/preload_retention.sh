#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q > ./data/retention.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

-- retention class
copy (
    select f.file_id, 'retention.class',
        '"' || 
            case 
                when dt.data_tier = 'raw' 
                    and ft.file_type_desc = 'detector'
                    and ds.datastream_name in ('physics', 'cosmics')
                then 'rawdata'

                when dt.data_tier = 'full-reconstructed' and ft.file_type_desc = 'detector'
                then 'physics'
    
                when dt.data_tier = 'full-reconstructed' and ft.file_type_desc = 'mc'
                then 'simulation'

                when ft.file_type_desc = 'detector' and ds.datastream_name = 'study'
                then 'study'

                when ds.datastream_name = 'test'
                then 'test'

                else 'other'
            end
        || '"'
        from active_files f 
            left outer join data_tiers dt on (f.data_tier_id = dt.data_tier_id)
            left outer join file_types ft on (f.file_type_id = ft.file_type_id)
            left outer join datastreams ds on (f.stream_id = ds.stream_id)
) to stdout;

copy (
    select f.file_id, 'retention.status', '"active"'
        from active_files f
) to stdout;


_EOF_

preload_json_meta ./data/retention.csv