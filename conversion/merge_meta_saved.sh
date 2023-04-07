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

create temp view i_attrs as
	select file_id, jsonb_object_agg(name, i) as obj
		from meta
		where type = 'i' and i is not null
		group by file_id
;

create temp view f_attrs as
	select file_id, jsonb_object_agg(name, f) as obj
		from meta
		where type = 'f' and f is not null
		group by file_id
;

create temp view t_attrs as
	select file_id, jsonb_object_agg(name, t) as obj
		from meta
		where type = 't' and t is not null
		group by file_id
;

create temp view ia_attrs as
	select file_id, jsonb_object_agg(name, ia) as obj
		from meta
		where type = 'ia' and ia is not null
		group by file_id
;

create temp view ta_attrs as
	select file_id, jsonb_object_agg(name, ta) as obj
		from meta
		where type = 'ta' and ta is not null
		group by file_id
;

create temp view combined_meta as
	select r.file_id, name, create_user as creator, to_timestamp(r.create_timestamp) as created_timestamp, size,
			coalesce(i.obj, '{}')::jsonb 
			|| coalesce(f.obj, '{}')::jsonb 
			|| coalesce(t.obj, '{}')::jsonb 
			|| coalesce(ia.obj, '{}')::jsonb 
			|| coalesce(ta.obj, '{}')::jsonb 
            as metadata
		from raw_files r
			left outer join i_attrs i on i.file_id = r.file_id
			left outer join f_attrs f on f.file_id = r.file_id
			left outer join t_attrs t on t.file_id = r.file_id
			left outer join ta_attrs ta on ta.file_id = r.file_id
			left outer join ia_attrs ia on ia.file_id = r.file_id
;

create index meta_file_id_inx on meta(file_id);

insert into files(id, namespace, name, creator, created_timestamp, size, metadata)
(
	select file_id, 'dune', name, creator, created_timestamp, size, metadata  
		from combined_meta
);



\echo ... creating primary key ...

alter table files add primary key(id);




_EOF_
