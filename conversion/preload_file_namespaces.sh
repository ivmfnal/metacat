#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/data_tiers.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;
                
create temp view file_rucio_scopes as
    select dfl.file_id, dsl.path
        from data_storage_locations dsl
        inner join data_file_locations dfl on dfl.location_id = dsl.location_id
        where dsl.location_type='rucio';
        
copy (
	select f.file_id, coalesce(s.path, 'default')
		from active_files f
            left outer join file_rucio_scopes s on s.file_id = f.file_id
) to stdout;



_EOF_

