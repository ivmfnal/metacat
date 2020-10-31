IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U samread -d sam_dune_prd"
#IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U ivm -d sam_dune_prd"
OUT_DB_PSQL="psql -h ifdb02.fnal.gov -d metadata"
DUNE_DB_PSQL="psql -h ifdb02.fnal.gov -d dune_metadata"

core_category="core"
default_namespace="default"

function create_meta_table () {
    $OUT_DB_PSQL << _EOF_

    create table if not exists meta (
        file_id text,
        name    text,
        value   jsonb
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

    create temp table meta_columns (
        file_id text,
        name    text,
        i       bigint,
        t       text,
        f       double precision,
        ia      bigint[],
        ta      text[]
    );

    \copy meta_columns (file_id, name, i, f, t, ia, ta) from '${input}';
    
    insert into meta (file_id, name, value) (
        select file_id, name, coalesce(
                to_jsonb(m.t), to_jsonb(m.f), to_jsonb(m.i), to_jsonb(m.ta), to_jsonb(m.ia)
            )
        from meta_columns
    );

_EOF_

}
