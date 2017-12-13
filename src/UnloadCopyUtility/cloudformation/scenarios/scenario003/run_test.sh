#!/usr/bin/env bash

source ${HOME}/variables.sh

DESCRIPTION="Perform Unload Copy with automatic password retrieval."
DESCRIPTION="${DESCRIPTION}Use a Python generated key for unload/copy rather than KMS generated key."
DESCRIPTION="${DESCRIPTION}Expect target location to be correct."
DESCRIPTION="${DESCRIPTION}Use Python2. Should fail for environment without pycrypto."

start_scenario "${DESCRIPTION}"

start_step "Create configuration JSON to copy ssb.dwdate of source cluster to public.dwdate on target cluster"
cat >${HOME}/scenario003.json <<EOF
{
  "unloadSource": {
    "clusterEndpoint": "${SourceClusterEndpointAddress}",
    "clusterPort": ${SourceClusterEndpointPort},
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "ssb",
    "tableName": "dwdate"
  },
  "s3Staging": {
    "aws_iam_role": "${S3CopyRole}",
    "path": "s3://${CopyUnloadBucket}/scenario003/",
    "deleteOnSuccess": "True",
    "region": "eu-west-1",
    "kmsGeneratedKey": "False"
  },
  "copyTarget": {
    "clusterEndpoint": "${TargetClusterEndpointAddress}",
    "clusterPort": ${TargetClusterEndpointPort},
    "connectUser": "${SourceClusterMasterUsername}",
    "db": "${SourceClusterDBName}",
    "schemaName": "public",
    "tableName": "dwdate"
  }
}
EOF

cat ${HOME}/scenario003.json >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Generate DDL for table public.dwdate on target cluster"
#Extract DDL
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select ddl from admin.v_generate_tbl_ddl where schemaname='ssb' and tablename='dwdate';" | awk '/CREATE TABLE/{flag=1}/ ;$/{flag=0}flag' | sed 's/ssb/public/' >${HOME}/scenario003.ddl.sql 2>>${STDERROR}
increment_step_result $?
cat ${HOME}/scenario003.ddl.sql >>${STDOUTPUT} 2>>${STDERROR}
increment_step_result $?
stop_step ${STEP_RESULT}

start_step "Drop table public.dwdate in target cluster if it exists"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "DROP TABLE IF EXISTS public.dwdate;" 2>>${STDERROR} | grep "DROP TABLE" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create table public.dwdate in target cluster"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -f ${HOME}/scenario003.ddl.sql | grep "CREATE TABLE" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


start_step "Run Unload Copy Utility"
source ${VIRTUAL_ENV_PY27_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd ${HOME}/amazon-redshift-utils/src/UnloadCopyUtility && python2 redshift_unload_copy.py ${HOME}/scenario003.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ssb.dwdate;" | grep "count=[0-9]*"|awk -F= '{ print $2}'` >>${STDOUTPUT} 2>>${STDERROR}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from public.dwdate;" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

stop_scenario