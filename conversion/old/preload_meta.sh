#!/bin/bash


input=$1
vtype=$2

source ./config.sh

$OUT_DB_PSQL << _EOF_

create temp table meta_csv (
	file_id	text,
	name	text,
    value	${vtype}
);

\echo imporing data ...

\copy meta_csv(file_id, name, value) from '${input}';

\echo inserting ...

insert into meta (file_id, meta)
(
    select f.file_id, jsonb_build_object(m.name, m.value)
        from meta_csv m, raw_files f
        where f.file_id = m.file_id
);

_EOF_