#!/usr/bin/env bash

source ${HOME}/variables.sh

start_scenario "Perform Unload Copy with password encrypted using KMS, expect target location to be correct on Python2, this has a dependency on scenario001"

start_step "Truncate the target table from previous scenario."
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "TRUNCATE TABLE public.dwdate;" 2>>${STDERROR} | grep "TRUNCATE TABLE" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


start_step "Run Unload Copy Utility"
source ${VIRTUAL_ENV_PY27_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd ${HOME}/amazon-redshift-utils/src/UnloadCopyUtility && python2 redshift_unload_copy.py ${HOME}/scenario001.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ssb.dwdate;" | grep "count=[0-9]*"|awk -F= '{ print $2}'`
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from public.dwdate;" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

stop_scenario