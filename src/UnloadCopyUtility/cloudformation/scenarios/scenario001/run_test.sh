#!/usr/bin/env bash

. ${HOME}/variables.sh

STEP_LABEL="Perform Unload Copy with password encrypted using KMS, expect target location to be correct"
scenario_result=0
start_scenario
STEP_LABEL="Create Unload Copy Manifest"
start_step
cat >scenario001.json <<EOF
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

cat scenario_001.json >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
scenario_result=$(( $scenario_result + $r ))

STEP_LABEL="Create DDL for table in target cluster"
start_step
#Extract DDL
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select ddl from admin.v_generate_tbl_ddl where schemaname='ssb' and tablename='dwdate';" | awk '/CREATE TABLE/{flag=1}/ ;$/{flag=0}flag' | sed 's/ssb/public/' >scenario001.ddl.sql
r=$? && stop_step $r
scenario_result=$(( $scenario_result + $r ))

STEP_LABEL="Create table in target cluster"
start_step
cat scenario001.ddl.sql >>${STDOUTPUT}
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -f scenario001.ddl.sql | grep "CREATE TABLE"
r=$? && stop_step $r
scenario_result=$(( $scenario_result + $r ))


STEP_LABEL="Run Unload Copy Utility"
start_step
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
cd /home/ec2-user/amazon-redshift-utils/src/UnloadCopyUtility && python3 redshift_unload_copy.py /home/ec2-user/scenario001.json eu-west-1 >>${STDOUTPUT} 2>>${STDERROR}
EXPECTED_COUNT=`psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'count='||count(*) from ssb.dwdate;" | grep "count=[0-9]*"|awk -F= '{ print $2}'
2556`
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'count='||count(*) from public.dwdate;" | grep "count=${EXPECTED_COUNT}" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
scenario_result=$(( $scenario_result + $r ))
deactivate

stop_scenario $scenario_result
