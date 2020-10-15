#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/data_tiers.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (

	select f.file_id, 'SAM.data_tier', 's', null, null, dt.data_tier 
		from active_files f, data_tiers dt 
		where f.data_tier_id = dt.data_tier_id
		order by f.file_id
) to stdout;



_EOF_
