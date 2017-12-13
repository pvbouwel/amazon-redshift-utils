#!/usr/bin/env bash

source ${HOME}/variables.sh

start_scenario "Perform Unload Copy with password encrypted using KMS, expect target location to be correct"
start_step "Create configuration JSON to copy ssb.dwdate of source cluster to public.dwdate on target cluster"
cat >${HOME}/scenario001.json <<EOF
{
  "unloadSource": {
    "clusterEndpoint": "${SourceClusterEndpointAddress}",
    "clusterPort": ${SourceClusterEndpointPort},
    "connectPwd": "${KMSEncryptedPassword}",
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "ssb",
    "tableName": "dwdate"
  },
  "s3Staging": {
    "aws_iam_role": "${S3CopyRole}",
    "path": "s3://${CopyUnloadBucket}/scenario1/",
    "deleteOnSuccess": "True",
    "region": "eu-west-1",
    "kmsGeneratedKey": "True"
  },
  "copyTarget": {
    "clusterEndpoint": "${TargetClusterEndpointAddress}",
    "clusterPort": ${TargetClusterEndpointPort},
    "connectPwd": "${KMSEncryptedPassword}",
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "public",
    "tableName": "dwdate"
  }
}
EOF

cat ${HOME}/scenario001.json >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Generate DDL for table public.dwdate on target cluster"
#Extract DDL
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select ddl from admin.v_generate_tbl_ddl where schemaname='ssb' and tablename='dwdate';" | awk '/CREATE TABLE/{flag=1}/ ;$/{flag=0}flag' | sed 's/ssb/public/' >${HOME}/scenario001.ddl.sql
increment_step_result $?
cat ${HOME}/scenario001.ddl.sql >>${STDOUTPUT} 2>>${STDERROR}
increment_step_result $?
stop_step ${STEP_RESULT}

start_step "Drop table public.dwdate in target cluster if it exists"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "DROP TABLE IF EXISTS public.dwdate;" 2>>${STDERROR} | grep "DROP TABLE"
r=$? && stop_step $r


start_step "Create table public.dwdate in target cluster"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -f ${HOME}/scenario001.ddl.sql | grep "CREATE TABLE"
r=$? && stop_step $r


start_step "Run Unload Copy Utility"
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd ${HOME}/amazon-redshift-utils/src/UnloadCopyUtility && python3 redshift_unload_copy.py ${HOME}/scenario001.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ssb.dwdate;" | grep "count=[0-9]*"|awk -F= '{ print $2}'`
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from public.dwdate;" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

stop_scenario