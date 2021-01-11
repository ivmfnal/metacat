#!/bin/bash

source config.sh

$DUNE_DB_PSQL -q > data/auth_info.csv << _EOF_
copy (
    select username, auth_info-'ldap' 
        from users 
        where auth_info-'ldap' != '{}'
    ) to stdout;
_EOF_

echo --- non-LDAP authenticators saved

