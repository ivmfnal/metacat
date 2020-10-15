#!/bin/sh


source ./config.sh

$IN_DB_PSQL -q > ./data/files.csv << _EOF_

copy (	select df.file_id, df.file_name, extract(epoch from df.create_date), p.username, df.file_size_in_bytes
		from data_files df
			left outer join persons p on p.person_id = df.create_user_id
		where df.retired_date is null 
) to stdout

_EOF_

$OUT_DB_PSQL << _EOF_

drop table if exists raw_files cascade;

create table raw_files
(
	file_id	    text,
	name		text,
	create_timestamp	double precision,
	create_user	text,
	size		bigint
);

truncate raw_files;

\echo importing raw files

\copy raw_files(file_id, name, create_timestamp, create_user, size) from 'data/files.csv';

\echo creating files index
create index raw_file_id on raw_files(file_id);

_EOF_
