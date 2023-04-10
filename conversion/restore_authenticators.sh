#!/bin/bash

source config.sh

if [ -z "$1" ]; then
	echo "Usage: $0 [-s <schema>] <postgres DB URL>"
	exit 1
fi

schema=public

if [ "$1" == "-s" ]; then
	schema=$2
	shift
	shift
fi

url=$1

echo URL: $url

psql -q $url << _EOF_
set search_path to $schema;
create temp table temp_auth( username text, auth_info jsonb, auid text );

\copy temp_auth(username, auth_info, auid) from 'data/auth_info.csv'

update users u
    set auth_info=coalesce(u.auth_info, '{}'::jsonb) || coalesce(t.auth_info, '{}'::jsonb),
        auid=coalesce(u.auid, t.auid)
    from temp_auth t
    where t.username = u.username
;

_EOF_

echo --- non-LDAP authenticators saved

