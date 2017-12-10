#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${DIR}/log_functionality.sh

start_scenario "Bootstrap the test EC2 instance and document environment"
scenario_result=0

start_step "Get commit information for this test report"
cd ${HOME}/amazon-redshift-utils
nr_of_lines=$(( `git log | grep -n '^commit ' | head -n 2 | tail -n 1 | awk -F: '{print $1}'` - 1 )) 2>>${STDERROR}
git log | head -n ${nr_of_lines} >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Install Python pip (easy_install pip)"
sudo easy_install pip >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Install OS packages (yum install -y postgresql postgresql-devel python27-virtualenv python36-devel python36-virtualenv gcc python-devel git aws-cli )"
sudo yum install -y postgresql postgresql-devel gcc python-devel python27-virtualenv python36-devel python36-virtualenv git aws-cli >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Get IAM_INFO.json"
curl http://169.254.169.254/latest/meta-data/iam/info > IAM_INFO.json 2>>${STDERROR}
echo "Result=`cat IAM_INFO.json`" >>${STDOUTPUT} 2>>${STDERROR}
cat IAM_INFO.json | grep Success &>>/dev/null
r=$? && stop_step $r

REGION_NAME=`curl http://169.254.169.254/latest/meta-data/hostname | awk -F. '{print $2}'`

start_step "Await full stack bootstrap"
STACK_NAME=`cat IAM_INFO.json | grep InstanceProfileArn | awk -F/ '{ print $2}'`
max_minutes_to_wait=15
minutes_waited=0
while [ 1 = 1 ]
do
    if [ "${minutes_waited}" = "${max_minutes_to_wait}" ]
    then
        stop_step 100
        break
    else
        aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} | grep StackStatus | grep CREATE_COMPLETE &>/dev/null
        if [ "$?" = "0" ]
        then
            stop_step 0
            break;
        else
            echo "`date` Stack not ready yet" >> ${STDOUTPUT}
            minutes_waited="$(( $minutes_waited + 1 ))"
            sleep 60
        fi
    fi
done

start_step "Get Cloudformation Stack name (aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME})"
aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} >STACK_DETAILS.json 2>>${STDERROR}
r=$? && stop_step $r

start_step "Setup Python2.7 environment"
echo 'VIRTUAL_ENV_PY27_DIR="${HOME}/venv_py27_env1/"' >> ${HOME}/variables.sh
source ${HOME}/variables.sh
virtualenv-2.7 ${VIRTUAL_ENV_PY27_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY27_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

start_step "Setup Python3.6 environment"
echo 'VIRTUAL_ENV_PY36_DIR="${HOME}/venv_py36_env1/"' >> ${HOME}/variables.sh
source ${HOME}/variables.sh
virtualenv-3.6 ${VIRTUAL_ENV_PY36_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate


start_step "Get all stack parameters (python ${DIR}/get_stack_parameters.py)"
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
python3 ${DIR}/get_stack_parameters.py >>${STDOUTPUT} 2>>${STDERROR}
grep "TargetClusterEndpointPort" $HOME/stack_parameters.json &>/dev/null
r=$? && stop_step $r

source ${HOME}/variables.sh

start_step "Create .pgpass files"
cat PASSWORD_KMS.txt | base64 --decode >>PASSWORD_KMS.bin 2>>${STDERROR}
CLUSTER_DECRYPTED_PASSWORD=`aws kms decrypt --ciphertext-blob fileb://PASSWORD_KMS.bin --region ${REGION_NAME} | grep Plaintext | awk -F\" '{print $4}' | base64 --decode` >>${STDOUTPUT} 2>>${STDERROR}
echo "${SourceClusterEndpointAddress}:${SourceClusterEndpointPort}:${SourceClusterDBName}:${SourceClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
echo "${TargetClusterEndpointAddress}:${TargetClusterEndpointPort}:${TargetClusterDBName}:${TargetClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
chmod 600  ${HOME}/.pgpass 2>>${STDERROR}
#Only verify that there are 2 records next we have test for access
cat ${HOME}/.pgpass | grep -v "::"| wc -l | grep "^2$" >>/dev/null 2>>${STDERROR}
r=$? && stop_step $r

#Needed because source is restored from snapshot.
start_step "Reset password of source cluster to CloudFormation Configuration"
if [ "${CLUSTER_DECRYPTED_PASSWORD}" = "" ]
then
    CLUSTER_DECRYPTED_PASSWORD=`aws kms decrypt --ciphertext-blob fileb://PASSWORD_KMS.bin --region ${REGION_NAME} | grep Plaintext | awk -F\" '{print $4}' | base64 --decode` >>${STDOUTPUT} 2>>${STDERROR}
fi
aws redshift modify-cluster --cluster-identifier "${SourceClusterName}" --master-user-password "${CLUSTER_DECRYPTED_PASSWORD}" --region "${Region}"  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Await no more pending modified variables"
return_code=1
while [ "$return_code" != "0" ]
do
  echo "There are variables to be modified on the cluster await cluster to be in sync"
  sleep 20
  aws redshift describe-clusters --cluster-identifier "${SourceClusterName}" --region "${Region}" | grep "\"PendingModifiedValues\": {}" >>/dev/null 2>>/dev/null
  return_code=$?
done
r=$? && stop_step $r

start_step "Test passwordless (.pgpass) access to source cluster"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'result='||1;" | grep "result=1" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Test passwordless (.pgpass) access to target cluster"
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'result='||1;" | grep "result=1" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r


#Setup admin tools
start_step "Create Admin schema on source if it does not exist"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "CREATE SCHEMA IF NOT EXISTS admin;" | grep "CREATE SCHEMA" >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

start_step "Create Admin view admin.v_generate_tbl_ddl on source if it does not exist"
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -f ${HOME}/amazon-redshift-utils/src/AdminViews/v_generate_tbl_ddl.sql | grep "CREATE VIEW"
r=$? && stop_step $r

SOURCE_CLUSTER_NAME=`grep -A 1 SourceClusterName STACK_DETAILS.json | grep OutputValue | awk -F\" '{ print $4}'`
start_step "Await Redshift restore of source cluster (${SOURCE_CLUSTER_NAME})"
max_minutes_to_wait=20
minutes_waited=0
while [ 1 = 1 ]
do
    if [ "${minutes_waited}" = "${max_minutes_to_wait}" ]
    then
        stop_step 100
        break
    else
        aws redshift describe-clusters --cluster-identifier ${SOURCE_CLUSTER_NAME} --region ${REGION_NAME} | grep -A 5  RestoreStatus | grep "\"Status\"" | grep completed >>/dev/null
        if [ "$?" = "0" ]
        then
            stop_step 0
            break;
        else
            echo "`date` Cluster restore not finished yet" >> ${STDOUTPUT}
            minutes_waited="$(( $minutes_waited + 1 ))"
            sleep 60
        fi
    fi
done

stop_scenario

#Start running the scenario's
for file in `find $DIR -type f -name 'run_test.sh'`
do
 log_section_action "Loading scenario file $file"
 . ${file}
done

#Publish results
echo "Publishing results to S3" >>${STDOUTPUT} 2>>${STDERROR}
aws s3 cp ${STDOUTPUT} "s3://${ReportBucket}/`date +%Y/%m/%d/%H/%M`/" >>${STDOUTPUT} 2>>${STDERROR}
aws s3 cp ${STDERROR} "s3://${ReportBucket}/`date +%Y/%m/%d/%H/%M`/" >>${STDOUTPUT} 2>>${STDERROR}
