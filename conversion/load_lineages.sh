#!/bin/sh


source ./config.sh

$IN_DB_PSQL -q > ./data/lineages.csv << _EOF_

copy (	select distinct l.file_id_source, l.file_id_dest
		from file_lineages l, data_files f1, data_files f2
		where f1.file_id = l.file_id_source and f1.retired_date is null 
			and f2.file_id = l.file_id_dest and f2.retired_date is null
) to stdout



_EOF_


$OUT_DB_PSQL << _EOF_

drop view if exists file_provenance, files_with_provenance;

drop table if exists parent_child;

create table parent_child
(
	parent_id text,
	child_id text
);

create temp table parent_child_temp
(
    like parent_child
);

\echo ... loading ...

\copy parent_child_temp(parent_id, child_id) from 'data/lineages.csv';

insert into parent_child(parent_id, child_id)
(
    select unique t.parent_id, t.child_id
    from parent_child_temp t
    inner join raw_files f1 on f1.file_id = t.parent_id
    inner join raw_files f2 on f2.file_id = t.child_id
);

\echo ... creating primary key ...



alter table parent_child add primary key(parent_id, child_id);


create view file_provenance as
    select f.id, 
        array(select parent_id from parent_child pc1 where pc1.child_id=f.id) as parents, 
        array(select child_id from parent_child pc2 where pc2.parent_id=f.id) as children
    from files f
;    

create view files_with_provenance as
    select f.*, r.children, r.parents
    from files f, file_provenance r
    where f.id = r.id
;



_EOF_
