#!/bin/bash

source config.sh

$DUNE_DB_PSQL -q > data/authenticators.csv << _EOF_
copy authenticators(username, type, secrets) to stdout;
_EOF_


