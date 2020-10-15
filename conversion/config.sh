IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U samread -d sam_dune_prd"
#IN_DB_PSQL="psql -h sampgsdb03.fnal.gov -p 5435 -U ivm -d sam_dune_prd"
OUT_DB_PSQL="psql -h ifdb02.fnal.gov -d metadata"

function create_meta_table () {
    $OUT_DB_PSQL << _EOF_

    create table if not exists meta (
        file_id text,
        name    text,
        type    text,
        i       bigint,
        t       text,
        f       double precision,
        ia      bigint[],
        ta      text[]
    );
_EOF_
}

function drop_meta_table () {
    $OUT_DB_PSQL << _EOF_
    drop table meta;
_EOF_
}


function preload_meta() {

    input=$1

    wc -l $input
    
    case $2 in
        text)   c=t
                t=text 
                ;;
        int)    c="i"
                t=bigint
                ;;
        float)  c="f"
                t="double precision"
                ;;
        int_a)  c="ia"
                t="bigint[]"
                ;;
        text_a) c="ta"
                t="text[]"
                ;;
    esac

    create_meta_table

    $OUT_DB_PSQL << _EOF_

    create temp table meta_csv (
    	file_id	text,
    	name	text,
    	value	${t}
    );

    \echo imporing metadata from ${input} ...

    \copy meta_csv(file_id, name, value) from '${input}';

    \echo inserting into meta table ...
    
    insert into meta (file_id, name, type, ${c})
    (
        select f.file_id, m.name, '${c}', m.value
            from meta_csv m, raw_files f
            where f.file_id = m.file_id
    );

_EOF_

}
