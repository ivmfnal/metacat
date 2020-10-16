#!/bin/sh


source ./config.sh

$IN_DB_PSQL -q > ./data/files.csv << _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null
;
                
--
-- create this as a table rather than a view so that we can try to create unique
-- index to make sure there is never more than 1 scope for a file
--

create temp table file_rucio_scopes 
(
    file_id bigint,
    scope   text
);

insert into file_rucio_scopes
(
    select dfl.file_id, dsl.path
        from data_storage_locations dsl
        inner join data_file_locations dfl on dfl.location_id = dsl.location_id
        where dsl.location_type='rucio'
);

create unique index file_rucio_scopes_unique on file_rucio_scopes(file_id, scope);

--
-- checksums
--

create temp view file_checksums as
    select df.file_id, jsonb_object(array_agg(array[ckt.checksum_name, ck.checksum_value])) as checksums
    from data_files df 
        inner join checksums ck on ck.file_id=df.file_id 
        inner join checksum_types ckt on ckt.checksum_type_id=ck.checksum_type_id 
    group by df.file_id
;

copy (	
    select df.file_id, coalesce(s.scope, 'default'), df.file_name, extract(epoch from df.create_date), p.username, df.file_size_in_bytes,
                coalesce(fck.checksums, '{}'::jsonb)
        from active_files df
            left outer join persons p on p.person_id = df.create_user_id
            left outer join file_rucio_scopes s on s.file_id = df.file_id
            left outer join file_checksums fck on fck.file_id = df.file_id
) to stdout

_EOF_

wc -l data/files.csv

$OUT_DB_PSQL << _EOF_

drop table if exists raw_files cascade;

create table raw_files
(
	file_id	    text,
    namespace   text,
	name		text,
	create_timestamp	double precision,
	create_user	text,
	size		bigint,
    checksums   jsonb
);

\echo importing raw files

\copy raw_files(file_id, namespace, name, create_timestamp, create_user, size, checksums) from 'data/files.csv';

\echo creating files index
create index raw_file_id on raw_files(file_id);

_EOF_
