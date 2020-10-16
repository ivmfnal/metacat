#!/bin/sh

source ./config.sh

$IN_DB_PSQL -q > ./data/data_tiers.csv << _EOF_

_EOF_

