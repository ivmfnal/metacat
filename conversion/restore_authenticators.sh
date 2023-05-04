#!/bin/bash

source config.sh

url=$DST_URL

echo Destination URL: $url

psql -q $url << _EOF_
create temp table temp_auth( username text, auth_info jsonb, auid text );

\copy temp_auth(username, auth_info, auid) from 'data/auth_info.csv'

update users u
    set auth_info=coalesce(u.auth_info, '{}'::jsonb) || coalesce(t.auth_info, '{}'::jsonb),
        auid=coalesce(u.auid, t.auid)
    from temp_auth t
    where t.username = u.username
;

create temp table temp_user_flags(username text, flags text);
\copy temp_user_flags(username, flags) from 'data/user_flags.csv'

select * from temp_user_flags;

update users u
    set flags=t.flags
    from temp_user_flags t
    where t.username = u.username
;


_EOF_

echo --- non-LDAP authenticators saved

