#!/bin/sh

source ./config.sh

cat data/app_families.csv  data/data_tiers.csv  data/file_formats.csv  data/file_types.csv  data/run_types.csv > data/meta_simple.csv


$OUT_DB_PSQL << _EOF_

drop table if exists attrs cascade;
drop table if exists meta cascade;

-- \echo creating attrs index
-- create index attrs_file_id on attrs(file_id);

\echo importing lbne.detector_type as lists

create temp table detector_type_lists (
    file_id text,
    value text[]
);

\copy detector_type_lists(file_id, value) from 'data/detector_type_lists.csv';

create temp view detector_type_as_json_list as
	select file_id, jsonb_object_agg('lbne_data.detector_type', value) as obj
		from detector_type_lists
		group by file_id
;



\echo importing runs/subruns

create temp table runs_subruns (
	file_id	text,
	name	text,
	value	bigint[]
);

\copy runs_subruns(file_id, name, value) from 'data/runs_subruns.csv';

create index rr_file_id on runs_subruns(file_id);

\echo importing attrs

create temp table attrs (
	file_id text,
	name	text,
        type	text,
	ivalue	bigint,
	fvalue	double precision,
	svalue	text
);

\copy attrs(file_id, name, type, ivalue, fvalue, svalue) from 'data/meta_simple.csv'

create temp view iattrs as
	select file_id, jsonb_object_agg(name, ivalue) as obj
		from attrs
		where type = 'i' and ivalue is not null
		group by file_id
;

create temp view fattrs as
	select file_id, jsonb_object_agg(name, fvalue) as obj
		from attrs
		where type = 'f' and fvalue is not null
		group by file_id
;

create temp view sattrs as
	select file_id, jsonb_object_agg(name, svalue) as obj
		from attrs
		where type = 's' and svalue is not null
		group by file_id
;

create temp view rr_merged as
	select file_id, jsonb_object_agg(name, value) as obj
		from runs_subruns
		group by file_id
;

create temp table meta (
	file_id text,
	meta	jsonb
);

\echo building meta ...

insert into meta (file_id, meta)
(
	select r.file_id, 
			coalesce(i.obj, '{}')::jsonb 
			|| coalesce(f.obj, '{}')::jsonb 
			|| coalesce(s.obj, '{}')::jsonb 
			|| coalesce(l.obj, '{}')::jsonb
            || coalesce(d.obj, '{}')::jsonb
		from raw_files r
			left outer join iattrs i on i.file_id = r.file_id
			left outer join fattrs f on f.file_id = r.file_id
			left outer join sattrs s on s.file_id = r.file_id
			left outer join rr_merged l on l.file_id = r.file_id
			left outer join detector_type_as_json_list d on d.file_id = r.file_id
);
			

\echo merging...

drop table if exists files;

create table files
(
        id text,
        namespace       text,
        name            text,
        creator     text,
        created_timestamp        timestamp with time zone,
        size            bigint,
        metadata        jsonb
);

insert into files(id, namespace, name, creator, created_timestamp, size, metadata)
(
	select f.file_id, 'dune', name, create_user, to_timestamp(f.create_timestamp), size, m.meta
		from raw_files f
			left outer join meta m on( f.file_id = m.file_id)
);

\echo ... creating primary key ...

alter table files add primary key(id);

alter table parent_child add foreign key(parent_id) references files(id);
alter table parent_child add foreign key(child_id)  references files(id);




_EOF_
