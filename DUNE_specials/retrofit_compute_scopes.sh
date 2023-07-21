#!/bin/bash

function compute()
{
    scope=$1
    shift
    where="$@ and namespace != $scope"
    echo scope: $scope where: $where
    metacat query -i files where $where > scope_${scope}.ids
    wc -l scope_${scope}.ids
}

compute protodune-sp core.data_tier=raw and core.run_type=protodune-sp &
compute pdsp_det_reco core.run_type=protodune-sp and core.file_type=detector and core.data_tier=full-reconstructed \
        and DUNE.campaign = 'PDSPProd4' and core.application.version = v09_09_01 and 'core.runs[any] >= 5204' \
        and core.data_stream = physics and data_quality.online_good_run_list = 1 &
compute pdsp_det_pandora core.run_type = 'protodune-sp' and core.file_type = detector and core.data_tier=pandora_info &
wait 

compute pdsp_det_tuple core.run_type = 'protodune-sp' and core.file_type = detector and core.data_tier=root-tuple &
compute pdsp_mc_detsim core.run_type = 'protodune-sp' and core.file_type = mc and core.data_tier=detector-simulated &
compute pdsp_mc_reco core.run_type = 'protodune-sp' and core.file_type = mc and core.data_tier=full-reconstructed &
wait

compute pdsp_mc_pandora core.run_type = 'protodune-sp' and core.file_type = mc and core.data_tier=pandora_info &
compute pdsp_mc_tuple core.run_type = 'protodune-sp' and core.file_type = mc and core.data_tier=root-tuple &
compute iceberg core.run_type = iceberg &
wait

compute mcc11 core.file_type = mc and DUNE.campaign = mcc11 and core.data_tier in '(detector-simulated, full-reconstructed)' &
compute vd-coldbox-bottom core.run_type = hd-coldbox and core.file_type = detector and core.data_tier=raw &
compute vd-coldbox-top core.run_type = vd-coldbox-top and core.file_type = detector and core.data_tier in '(raw , full-reconstructed)' &
wait

compute fd_vd_mc_reco core.file_type = mc and DUNE.campaign in '(FDVDProd1,FDVDProd2)' \
    and core.data_tier in '(root-tuple,full-reconstructed)' and core.run_type = fardet-vd &
compute hd-coldbox core.run_type = hd-coldbox and core.file_type = detector and core.data_tier=raw &
compute hd-protodune core.run_type = hd-protodune
wait

compute mcc10 core.file_type=mc and lbne_mc.production_campaign=mcc10.0 &
compute dc4-vd-coldbox-bottom core.run_type=dc4-vd-coldbox-bottom &
compute dc4-vd-coldbox-top core.run_type=dc4-vd-coldbox-top &
wait 

compute dc4-hd-protodune core.run_type=dc4-hd-protodune &
compute lbne core.group=lbne &
wait

