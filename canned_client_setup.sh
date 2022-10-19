# edit and soutrce this file

export METACAT_SERVER_URL=...
export METACAT_AUTH_SERVER_URL=...

METACAT_CLIENT_ROOT=...         # where you untared the metacat_client_...tar

# to use dependencies installed with pip or otherwise
export PYTHONPATH=${METACAT_CLIENT_ROOT}/lib

# to use canned dependencies, add dependencies subdirectory
# export PYTHONPATH=${METACAT_CLIENT_ROOT}/lib:${METACAT_CLIENT_ROOT}/dependencies

export PATH=${METACAT_CLIENT_ROOT}/ui