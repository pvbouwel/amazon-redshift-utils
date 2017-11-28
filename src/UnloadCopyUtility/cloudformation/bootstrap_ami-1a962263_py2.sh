#!/bin/bash
STDOUTPUT="output.log"
STDERROR="output.error"
SECTION_SEPARATOR=">>>SECTION:"
STEP_COUNTER=0
STEP_LABEL=""

function log_section_action() {
  # $1 will be the action
  echo "${SECTION_SEPARATOR}${STEP_COUNTER}:${STEP_LABEL}:$1" >>$STDOUTPUT
  echo "${SECTION_SEPARATOR}${STEP_COUNTER}:${STEP_LABEL}:$1" >>$STDERROR

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


STEP_LABEL="Get IAM_INFO.json"
start_step
curl http://169.254.169.254/latest/meta-data/iam/info > IAM_INFO.json 2>>$STDERROR
echo "Result=`cat IAM_INFO.json`" >>$STDOUT
cat IAM_INFO.json | grep Success &>>/dev/null
r=$? && stop_step $r

STEP_LABEL="Get STACK details"
STACK_NAME=`cat IAM_INFO.json | grep InstanceProfileArn | awk -F/ '{ print $2}'`
REGION_NAME=`curl http://169.254.169.254/latest/meta-data/hostname | awk -F. '{print $2}'`
STEP_LABEL="Get Cloudfromation Stack name (aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME})"
aws cloudformation describe-stacks --region ${REGION_NAME} --stack-name ${STACK_NAME} >STACK_DETAILS.json 2>>$STDERROR
r=$? && stop_step $r


STEP_LABEL="Install Python pip (easy_install pip)"
start_step
easy_install pip >>$STDOUTPUT 2>>$STDERROR
r=$? && stop_step $r

STEP_LABEL="Install OS packages (yum install -y postgresql postgresql-devel gcc python-devel git aws-cli)"
start_step
yum install -y postgresql postgresql-devel gcc python-devel git aws-cli >>$STDOUTPUT 2>>$STDERROR
r=$? && stop_step $r

STEP_LABEL="Install PyGreSQL using pip (pip install PyGreSQL)"
start_step
pip install PyGreSQL  >>$STDOUTPUT 2>>$STDERROR
r=$? && stop_step $r