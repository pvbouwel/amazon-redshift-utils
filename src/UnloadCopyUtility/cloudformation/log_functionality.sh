#!/usr/bin/env bash

STDOUTPUT="output.log"
STDERROR="output.error"
SECTION_SEPARATOR=">>>SECTION:"
STEP_LABEL=""
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


set | grep STEP_COUNTER
if [ "$?" != "0" ]
then
  STEP_COUNTER=0
fi

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