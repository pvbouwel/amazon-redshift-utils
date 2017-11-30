#!/usr/bin/env bash

VIRTUAL_ENV_PY36_DIR="${HOME}/venv_py36_env1/"
VIRTUAL_ENV_PY27_DIR="${HOME}/venv_py27_env1/"

#If bash variables are present in home directory source those
if [ -f "${HOME}/variables.sh" ]
then
  . ${HOME}/variables.sh
fi