#!/usr/bin/env bash

STDOUTPUT="${HOME}/output.log"
STDERROR="${HOME}/output.error"
STEP_LABEL=""
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


function log_event_for_type() {
  EVENT="$1"
  TYPE="$2"
  date=`date +"%d/%m/%Y %H:%M:%S.%N"`
  echo ">>>${TYPE}:${date}:${STEP_LABEL}:${EVENT}" >>${STDOUTPUT}
  echo ">>>${TYPE}:${date}:${STEP_LABEL}:${EVENT}" >>${STDERROR}

}

function start_for_type() {
  TYPE="$1"
  log_event_for_type "START" "${TYPE}"
}

function start_step() {
  start_for_type "STEP"
}
function start_scenario() {
  start_for_type "SCENARIO"
}

function stop_for_type_with_return_code() {
  TYPE="$1"
  RETURN_CODE="$2"
  if [ "${RETURN_CODE}" == "0" ]
  then
    log_event_for_type "STOP SUCCEEDED" "${TYPE}"
  else
    log_event_for_type "STOP FAILED WITH RETURN_CODE ${RETURN_CODE}" "${TYPE}"
  fi
}

function stop_step() {
  RETURN_CODE="$1"
  stop_for_type_with_return_code "STEP" ${RETURN_CODE}
}
function stop_scenario() {
  RETURN_CODE="$1"
  stop_for_type_with_return_code "SCENARIO" ${RETURN_CODE}
}
