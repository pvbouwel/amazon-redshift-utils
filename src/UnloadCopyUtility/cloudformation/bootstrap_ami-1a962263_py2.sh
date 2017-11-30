#!/bin/bash
STDOUTPUT="output.log"
STDERROR="output.error"
SECTION_SEPARATOR=">>>SECTION:"
STEP_COUNTER=0
STEP_LABEL=""

function log_section_action() {
  # $1 will be the action
  date=`date`
  echo "${SECTION_SEPARATOR}${STEP_COUNTER}:${date}:${STEP_LABEL}:$1" >>${STDOUTPUT}
  echo "${SECTION_SEPARATOR}${STEP_COUNTER}:${date}:${STEP_LABEL}:$1" >>${STDERROR}

}

function start_step() {
  STEP_COUNTER="$(( $STEP_COUNTER + 1 ))"
  log_section_action "START"
}

function stop_step() {
  if [ "$1" == "0" ]
  then
    log_section_action "STEP SUCCEEDED"
  else
    log_section_action "STEP FAILED WITH RETURN_CODE $1"
  fi
}

STEP_LABEL="Install Python pip (easy_install pip)"
start_step
easy_install pip >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Install OS packages (yum install -y postgresql postgresql-devel gcc python-devel git aws-cli)"
start_step
yum install -y postgresql postgresql-devel gcc python-devel git aws-cli >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Install PyGreSQL using pip (pip install PyGreSQL)"
start_step
pip install PyGreSQL  >>${STDOUTPUT} 2>>${STDERROR}
r=$? && stop_step $r

STEP_LABEL="Get IAM_INFO.json"
start_step
curl http://169.254.169.254/latest/meta-data/iam/info > IAM_INFO.json 2>>${STDERROR}
echo "Result=`cat IAM_INFO.json`" >>${STDOUTPUT}
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
STEP_LABEL="Get all stack parameters (python ${DIR}/get_stack_parameters.py)"
start_step
python ${DIR}/get_stack_parameters.py
cat $HOME/stack_parameters.json
grep "TargetClusterEndpointPort" $HOME/stack_parameters.json &>/dev/null
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
        aws redshift describe-clusters --cluster-identifier ${SOURCE_CLUSTER_NAME} --region ${REGION_NAME} | grep -A 5  RestoreStatus | grep "\"Status\"" | grep completed
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