#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${DIR}/log_functionality.sh

STEP_LABEL="Install Python pip (easy_install pip)"
start_step
easy_install pip >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Install OS packages (yum install -y postgresql postgresql-devel python27-virtualenv python36-devel python36-virtualenv gcc python-devel git aws-cli )"
start_step
yum install -y postgresql postgresql-devel gcc python-devel python27-virtualenv python36-devel python36-virtualenv git aws-cli >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Install PyGreSQL using pip (pip install PyGreSQL)"
start_step
pip install PyGreSQL  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Get IAM_INFO.json"
start_step
curl http://169.254.169.254/latest/meta-data/iam/info > IAM_INFO.json 2>>${STDERROR}
echo "Result=`cat IAM_INFO.json`" >>${STDOUTPUT} 2>>${STDERROR}
cat IAM_INFO.json | grep Success &>>/dev/null
r=$? && stop_step $r

REGION_NAME=`curl http://169.254.169.254/latest/meta-data/hostname | awk -F. '{print $2}'`

STEP_LABEL="Await full stack bootstrap"
start_step
STACK_NAME=`cat IAM_INFO.json | grep InstanceProfileArn | awk -F/ '{ print $2}'`
max_minutes_to_wait=15
minutes_waited=0
while [ 1 = 1 ]
do
    if [ ${minutes_waited} = ${max_minutes_to_wait} ]
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
            minutes_waited="$( $minutes_waited + 1 )"
            sleep 60
        fi
    fi
done

STEP_LABEL="Get STACK details"
start_step
STEP_LABEL="Get Cloudformation Stack name (aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME})"
aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} >STACK_DETAILS.json 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Setup Python2.7 environment"
start_step
source ${DIR}/variables.sh
virtualenv-2.7 ${VIRTUAL_ENV_PY27_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY27_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate

STEP_LABEL="Setup Python3.6 environment"
start_step
source ${DIR}/variables.sh
virtualenv-3.6 ${VIRTUAL_ENV_PY36_DIR} >>${STDOUTPUT} 2>>${STDERROR}
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
pip install -r ${DIR}/requirements.txt >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r
deactivate


STEP_LABEL="Get all stack parameters (python ${DIR}/get_stack_parameters.py)"
start_step
source ${VIRTUAL_ENV_PY36_DIR}/bin/activate >>${STDOUTPUT} 2>>${STDERROR}
python3 ${DIR}/get_stack_parameters.py >>${STDOUTPUT} 2>>${STDERROR}
grep "TargetClusterEndpointPort" $HOME/stack_parameters.json &>/dev/null
r=$? && stop_step $r

source ${DIR}/variables.sh

STEP_LABEL="Create .pgpass files"
start_step
cat PASSWORD_KMS.txt | base64 --decode >>PASSWORD_KMS.bin 2>>${STDERROR}
CLUSTER_DECRYPTED_PASSWORD=`aws kms decrypt --ciphertext-blob fileb://PASSWORD_KMS.bin --region ${REGION_NAME} | grep Plaintext | awk -F\" '{print $4}' | base64 --decode` >>${STDOUTPUT} 2>>${STDERROR}
echo "${SourceClusterEndpointAddress}:${SourceClusterEndpointPort}:${SourceClusterDBName}:${SourceClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
echo "${TargetClusterEndpointAddress}:${TargetClusterEndpointPort}:${TargetClusterDBName}:${TargetClusterMasterUsername}:${CLUSTER_DECRYPTED_PASSWORD}" >> ${HOME}/.pgpass 2>>${STDERROR}
chmod 600  ${HOME}/.pgpass 2>>${STDERROR}
#Only verify that there are 2 records next we have test for access
cat ${HOME}/.pgpass | grep -v "::"| wc -l | grep "^2$" >>/dev/null 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Test passwordless (.pgpass) access to source cluster"
start_step
psql -h ${SourceClusterEndpointAddress} -p ${SourceClusterEndpointPort} -U ${SourceClusterMasterUsername} ${SourceClusterDBName} -c "select 'result='||1;" | grep "result=1"
r=$? && stop_step $r

STEP_LABEL="Test passwordless (.pgpass) access to target cluster"
start_step
psql -h ${TargetClusterEndpointAddress} -p ${TargetClusterEndpointPort} -U ${TargetClusterMasterUsername} ${TargetClusterDBName} -c "select 'result='||1;" | grep "result=1"
r=$? && stop_step $r

SOURCE_CLUSTER_NAME=`grep -A 1 SourceClusterName STACK_DETAILS.json | grep OutputValue | awk -F\" '{ print $4}'`
STEP_LABEL="Await Redshift restore of source cluster (${SOURCE_CLUSTER_NAME})"
start_step
max_minutes_to_wait=20
minutes_waited=0
while [ 1 = 1 ]
do
    if [ ${minutes_waited} = ${max_minutes_to_wait} ]
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
            minutes_waited="$( $minutes_waited + 1 )"
            sleep 60
        fi
    fi
done


#Start running the scenario's
for file in `find $DIR -type f -name 'run_test.sh'`
do
 log_section_action "Loading scenario file $file"
 . ${file}
done