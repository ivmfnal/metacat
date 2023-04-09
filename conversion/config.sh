SRC_URL="postgresql://samread@sampgsdb03.fnal.gov:5435/sam_dune_prd"
DST_URL="postgresql://ivm@ifdbprod.fnal.gov.fnal.gov:5463/dune_metadata_prd"

SRC_SCHEMA=""
DST_SCHEMA=""

if [ ! -z "$SRC_SCHEMA" ]; then
    SRC_URL="${SRC_URL}?options=-c%20search_path%3d${SRC_SCHEMA}"
fi

if [ ! -z "$DST_SCHEMA" ]; then
    DST_URL="${DST_URL}?options=-c%20search_path%3d${DST_SCHEMA}"
fi

#IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U samread -d sam_dune_prd"
#OUT_DB_PSQL="psql -h ifdbprod.fnal.gov -p 5463 -d dune_metadata_prd"

IN_DB_PSQL="psql -v on_error_stop=on \"${SRC_URL}\""
OUT_DB_PSQL="psql -v on_error_stop=on \"${DST_URL}\""

core_category="core"
origin_category="origin"
default_namespace="dune"

function create_meta_table () {
    $OUT_DB_PSQL -q << _EOF_

    \set on_error_stop on

    create table if not exists meta (
        file_id text,
        name    text,
        value   jsonb
    );
_EOF_
}

function init_destination () {
    $OUT_DB_PSQL << _EOF_
        drop table if exists meta;
        drop table if exists raw_files;
        drop table if exists files cascade;
        drop table if exists files_datasets, parameter_definitions, parent_child, datasets_parent_child cascade, users_roles;
        drop table if exists namespaces, parameter_categories, queries cascade;
        drop table if exists files, datasets cascade;
        drop table if exists users, roles cascade;    
_EOF_
}


function preload_meta() {

    input=$1
    create_meta_table

    $OUT_DB_PSQL << _EOF_

    \set on_error_stop on
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
        from meta_columns m
    );

_EOF_

}

function preload_json_meta() {
    
    input=$1
    create_meta_table
    wc -l $input
    #echo loading `wc -l $input` lines of metadata from $input ... 
    
    $OUT_DB_PSQL << _EOF_

    \set on_error_stop on
    \copy meta from '${input}';

_EOF_
}
