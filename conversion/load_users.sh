#!/bin/bash

source ./config.sh

$IN_DB_PSQL -q > ./data/users.csv << _EOF_

copy (	select username, first_name || ' ' ||  last_name, email_address
	from persons
) to stdout

_EOF_

$IN_DB_PSQL -q > ./data/roles.csv << _EOF_

copy (	select work_grp_name from working_groups) to stdout;

_EOF_

$IN_DB_PSQL -q > ./data/users_roles.csv << _EOF_

copy (
    select p.username, wg.work_grp_name
        from persons_working_groups pwg
            inner join persons p on p.person_id = pwg.person_id
            inner join working_groups wg on wg.work_grp_id = pwg.work_grp_id
) to stdout;

_EOF_

wc -l ./data/users_roles.csv

$OUT_DB_PSQL -q << _EOF_

drop table if exists users_roles cascade;
drop table if exists users cascade;
drop table if exists roles cascade;

create table users
(
    username    text    primary key,
    name        text,
    email       text,
    flags       text    default '',
    auth_info   jsonb   default '{}'
);

create table roles
(
    name        text    primary key,
    description text
);

create table users_roles
(
    username    text    references users(username),
    role_name   text    references roles(name),
    primary key(username, role_name)
);

\copy users (username, name, email) from 'data/users.csv';

update users 
    set auth_info=auth_info || (
        '{"ldap":"cn='  ||  username  ||  ',ou=FermiUsers,dc=services,dc=fnal,dc=gov"}'
    )::jsonb 
;
    
update users 
    set auth_info=auth_info || '{"x509":[]}'::jsonb 
;
    
\copy roles(name) from 'data/roles.csv';

\copy users_roles(username, role_name) from 'data/users_roles.csv';

insert into users(username, name, flags)
	values('admin','Admin user', 'a');
insert into users_roles(username, role_name) values ('admin','admin_role');



_EOF_
