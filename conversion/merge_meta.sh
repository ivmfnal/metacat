#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_
drop table if exists files cascade;

create table files          -- without any references at this time
(
    id          text,
    namespace   text,
    name        text,
    metadata    jsonb   default '{}',
    creator     text,
    size        bigint,
    checksums   jsonb   default '{}',
    created_timestamp   timestamp with time zone    default now(),
    updated_by  text,
    updated_timestamp   timestamp with time zone    default now(),
    retired     boolean default false,
    retired_timestamp   timestamp with time zone,
    retired_by  text
);


create temp view combined_meta as
	select m.file_id, jsonb_object_agg(m.name, m.value) as metadata
		from meta m
		group by file_id
;



insert into files(id, namespace, name, creator, created_timestamp, updated_by, updated_timestamp, size, checksums, metadata)
(
	select r.file_id, r.namespace, r.name, 
                        r.create_user, to_timestamp(r.create_timestamp), 
                        r.update_user, to_timestamp(r.update_timestamp), 
                        r.size, r.checksums, coalesce(m.metadata, '{}'::jsonb)  
		from raw_files r
			left outer join combined_meta m on (m.file_id = r.file_id)
);

alter table files add primary key(id);

_EOF_
