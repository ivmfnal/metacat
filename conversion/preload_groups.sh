#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q > ./data/groups.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy (
	select f.file_id, '${core_category}.group', to_json(wg.work_grp_name)
		from active_files f, working_groups wg
		where f.responsible_working_group_id = wg.work_grp_id
) to stdout;

_EOF_

preload_json_meta ./data/groups.csv
