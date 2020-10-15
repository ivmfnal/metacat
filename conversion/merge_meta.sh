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
        created_timestamp        timestamp with time zone,
        size            bigint,
        metadata        jsonb
);

create temp view combined_meta as
	select m.file_id, jsonb_object_agg(m.name, coalesce(to_jsonb(m.t), to_jsonb(m.f),to_jsonb(m.i),to_jsonb(m.ta),to_jsonb(m.ia))) as metadata
		from meta m
		group by file_id
;

-- create index meta_file_id_inx on meta(file_id);

insert into files(id, namespace, name, creator, created_timestamp, size, metadata)
(
	select r.file_id, 'dune', r.name, r.create_user, to_timestamp(r.create_timestamp), r.size, m.metadata  
		from raw_files r
			left outer join combined_meta m on (m.file_id = r.file_id)
);

\echo ... creating primary key ...

alter table files add primary key(id);




_EOF_
