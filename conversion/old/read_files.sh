#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/files.csv << _EOF_

copy (	select df.file_id, df.file_name, extract(epoch from df.create_date), p.username, df.file_size_in_bytes
		from data_files df
			left outer join persons p on p.person_id = df.create_user_id
		where df.retired_date is null 
                order by df.file_id
) to stdout



_EOF_

