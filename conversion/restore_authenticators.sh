#!/bin/bash

source config.sh

$DUNE_DB_PSQL << _EOF_
create temp table temp_auth( like authenticators );
\copy temp_auth(username, type, secrets) from 'data/authenticators.csv'

delete from authenticators where username in (select username from temp_auth);

insert into authenticators(username, type, secrets) (
	select username, type, secrets from temp_auth
);
_EOF_
