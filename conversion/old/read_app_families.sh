#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/app_families.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

create temp table attrs
(
	file_id	bigint,
	name text,
	value text
);

insert into attrs(file_id, name, value)
(
	select f.file_id, 'SAM.application.version', a.version
	from active_files f, application_families a where a.appl_family_id = f.appl_family_id
);

insert into attrs(file_id, name, value)
(
	select f.file_id, 'SAM.application.family', a.family
	from active_files f, application_families a where a.appl_family_id = f.appl_family_id
);

insert into attrs(file_id, name, value)
(
	select f.file_id, 'SAM.application.name', a.appl_name
	from active_files f, application_families a where a.appl_family_id = f.appl_family_id
);

insert into attrs(file_id, name, value)
(
	select f.file_id, 'SAM.application', a.family || '.' || a.appl_name
	from active_files f, application_families a where a.appl_family_id = f.appl_family_id
);


copy (
	select file_id, name,  's', null, null, value
		from attrs
		order by file_id
) to stdout;







_EOF_
