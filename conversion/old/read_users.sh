#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/users.csv << _EOF_

copy (	select username, first_name || ' ' ||  last_name, email_address
	from persons
) to stdout



_EOF_

