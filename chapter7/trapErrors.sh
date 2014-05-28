#!/bin/sh

# Configure bash behaviour
set -o pipefail         # Trace errors through pipes
set -o errtrace         # Trace ERR through 'time command'

error() {
  JOB="$0"              # job name
  LASTLINE="$1"         # line of error occurrence
  LASTERR="$2"          # error code
  echo "ERROR in ${JOB}:line ${LASTLINE} - exit code ${LASTERR}"
  exit 1
}
trap 'error ${LINENO} ${$?}' ERR
# hadoop jar ...

wrong-command
echo "correct command"