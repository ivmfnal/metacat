#!/bin/bash

source ./config.sh

$OUT_DB_PSQL << _EOF_
        -- create user ${WEB_USER};
        grant all on schema ${DST_SCHEMA} to ${WEB_USER};
        grant all on all tables in schema ${DST_SCHEMA} to ${WEB_USER};
        \echo
        \echo Finalized
        \echo
_EOF_


