#!/bin/bash

source ./config.sh

rm  data/*.csv
drop_tables

echo Starting parallel preloading at `date` ...

(
    echo
    echo -0- loading users ...
    time ./load_users.sh

    echo
    echo -0- loading raw files ...
    time ./load_files.sh

    echo -0- DONE
)&

(
    echo
    echo -1- preloading attributes ...
    ./preload_attrs.sh

    echo
    echo -1- preloading event numbers
    ./preload_event_numbers.sh

    echo -1- DONE

)&

(
    echo
    echo -2- preloading DUNE_data.detector_config values as JSON objects ...
    ./preload_detector_config.sh

    echo
    echo -2- splitting lbne_data.detector_type values into lists ...
    ./preload_detector_type.sh

    echo
    echo -2- preload runs/subruns ...
    ./preload_runs_subruns.sh

    echo -2- DONE

)&

(
    echo
    echo -3- preloading app families ...
    ./preload_app_families.sh

    echo
    echo -3- preloading data streams ...
    ./preload_data_streams.sh

    echo
    echo -3- preloading content status ...
    ./preload_content_status.sh

    echo
    echo -3- preloading file formats ...
    ./preload_formats.sh

    echo
    echo -3- preloading file types ...
    ./preload_file_types.sh

    echo
    echo -3- preloading run types ...
    ./preload_run_types.sh

    echo
    echo -3- preloading data tiers ...
    ./preload_data_tiers.sh

    echo -3- DONE

)&

echo Waiting for parallel paths to join ...

wait
echo Parallel preloading finished at `date` 

echo
echo --- merging metadata into the files table ...
time ./merge_meta.sh

echo
echo --- loading lineages ...
time ./load_lineages.sh

echo
echo --- creating other tables ...
time ./create_other_tables.sh

echo
echo --- building indexes ...
time ./build_indexes.sh

echo
echo --- building foreign keys ...
time ./build_foreign_keys.sh




