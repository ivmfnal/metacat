#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q \
	> ./data/attrs.csv \
	<< _EOF_

create temp view active_files as
        select * from data_files
                where retired_date is null;

create temp table attrs (
	file_id bigint,
	name	text,
        type	text,
	ivalue	bigint,
	fvalue	double precision,
	svalue	text
);

insert into attrs(file_id, name, type, ivalue)
(  select df.file_id, 'SAM.event_count', 'i', df.event_count
                from active_files df
                where df.event_count is not null
   union
   select df.file_id, 'SAM.first_event_number', 'i', df.first_event_number
                from active_files df
                where df.first_event_number is not null
   union
   select df.file_id, 'SAM.last_event_number', 'i', df.last_event_number
                from active_files df
                where df.last_event_number is not null
);

insert into attrs(file_id, name, type, fvalue)
(
   select df.file_id, 'SAM.start_time', 'f', extract(epoch from df.start_time)
                from active_files df
                where df.start_time is not null
   union
   select df.file_id, 'SAM.end_time', 'f', extract(epoch from df.end_time)
                from active_files df
                where df.end_time is not null
) ;

insert into attrs(file_id, name, type, ivalue)
( 
	select f.file_id, pc.param_category || '.' || pt.param_type, 'i', param_value
                from active_files f
                inner join num_data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type_id = 5
                    and param_value is not null

);


insert into attrs(file_id, name, type, fvalue)
( 
	select f.file_id, pc.param_category || '.' || pt.param_type, 'f', param_value
                from active_files f
                inner join num_data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type_id = 6
                    and param_value is not null
);

-- string attrs
insert into attrs(file_id, name, type, svalue)
( select f.file_id, pc.param_category || '.' || pt.param_type, 't', pv.param_value
                from active_files f
                inner join data_files_param_values dfv on f.file_id = dfv.file_id
                inner join param_values pv on pv.param_value_id = dfv.param_value_id
                inner join param_types pt on pt.param_type_id = dfv.param_type_id
                inner join data_types dt on dt.data_type_id = pt.data_type_id
                inner join param_categories pc on pc.param_category_id = pt.param_category_id
                where dt.data_type = 'string'
                    and pv.param_value is not null
);


copy (select file_id, name, type, ivalue, fvalue, svalue
        from attrs 
) to stdout;

_EOF_

$OUT_DB_PSQL << _EOF_

\copy meta(file_id, name, type, i, f, t) from 'data/attrs.csv'

_EOF_



