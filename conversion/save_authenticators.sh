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

psql -q $url > data/auth_info.csv << _EOF_
set search_path to $schema;
copy (
    select username, auth_info-'ldap' 
        from users 
        where auth_info-'ldap' != '{}'
    ) to stdout;
_EOF_

echo --- non-LDAP authenticators saved

