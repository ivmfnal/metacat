#!/bin/bash

source config.sh

$DUNE_DB_PSQL << _EOF_
create temp table temp_auth( username text, auth_info jsonb );

\copy temp_auth(username, auth_info) from 'data/auth_info.csv'

update users u
    set auth_info=coalesce(u.auth_info, '{}'::json) ||
        (
            select a.auth_info from temp_auth a where a.username=u.username
        )
        where exists ( select * from temp_auth a where a.username = u.username)
;

_EOF_
