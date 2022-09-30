#!/bin/sh

source ./config.sh

$OUT_DB_PSQL << _EOF_

\echo merging metadata ...

drop table if exists files cascade;

create table files
(
        id text,
        namespace       text,
        name            text,
        creator         text,
        created_timestamp        timestamp with time zone   default now(),
        updated_by         text,
        updated_timestamp        timestamp with time zone   default now(),
        size            bigint  default 0,
        checksums       jsonb   default '{}',
        metadata        jsonb   default '{}'
);

create temp view combined_meta as
	select m.file_id, jsonb_object_agg(m.name, m.value) as metadata
		from meta m
		group by file_id
;

insert into files(id, namespace, name, creator, created_timestamp, size, checksums, metadata)
(
	select r.file_id, r.namespace, r.name, 
                        r.create_user, to_timestamp(r.create_timestamp), 
                        r.update_user, to_timestamp(r.update_timestamp), 
                        r.size, r.checksums, m.metadata  
		from raw_files r
			left outer join combined_meta m on (m.file_id = r.file_id)
);

\echo ... creating primary key ...

alter table files add primary key(id);




_EOF_
