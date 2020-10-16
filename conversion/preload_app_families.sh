#!/bin/sh

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
	select f.file_id, '.application.version', null, null, a.version, null, null
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '.application.family', null, null, a.family, null, null
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '.application.name', null, null, a.appl_name, null, null
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

copy
(
	select f.file_id, '.application', null, null, a.family || '.' || a.appl_name, null, null
	    from active_files f, application_families a where a.appl_family_id = f.appl_family_id
) to stdout;

_EOF_


preload_meta ./data/app_families.csv

