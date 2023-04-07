#!/bin/bash -i

source ./config.sh

echo dumping data ...

$IN_DB_PSQL -q \
	> ./data/app_families.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

copy
(
	select f.file_id, '${core_category}.application.version', to_json(a.version)
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '${core_category}.application.family', to_json(a.family)
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '${core_category}.application.name', to_json(a.appl_name)
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '${core_category}.application', to_json(a.family || '.' || a.appl_name)
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

_EOF_


preload_json_meta ./data/app_families.csv

