IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U samread -d sam_dune_prd"
#IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U ivm -d sam_dune_prd"
OUT_DB_PSQL="psql -h ifdb02.fnal.gov -d metadata"

function create_meta_table () {
    $OUT_DB_PSQL << _EOF_

    create table if not exists meta (
        file_id text,
        name    text,
        i       bigint,
        t       text,
        f       double precision,
        ia      bigint[],
        ta      text[]
    );
_EOF_
}

function drop_tables () {
    $OUT_DB_PSQL << _EOF_
    drop table if exists meta;
    drop table if exists raw_files;
    drop table if exists files cascade;
_EOF_
}


function preload_meta() {

    input=$1
    create_meta_table

    $OUT_DB_PSQL << _EOF_

    \echo imporing metadata from ${input} ...

    \copy meta (file_id, name, i, f, t, ia, ta) from '${input}';

_EOF_

}
