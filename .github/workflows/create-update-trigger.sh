#!/usr/bin/env bash

usage="Usage: $(basename "$0") job-name trigger-name trigger-type
where:
  job-name          - the job name
  trigger-name      - the trigger name
  trigger-type      - the trigger type
"

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "help" ] || [ "$1" == "usage" ] ; then
  echo "$usage"
  exit -1
fi

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
  echo "$usage"
  exit -1
fi


# shopt -s failglob
# set -eu -o pipefail
set -e

JOB_NAME="$1"
TRIGGER_NAME="$2"
TRIGGER_TYPE="$3"
TRIGGER_CRONJOB="$4"

echo -e JOB_NAME: $JOB_NAME
response=$(aws glue get-triggers --dependent-job-name $JOB_NAME)

echo -e response: "${response}"

if [[ $response == *"$TRIGGER_NAME"* ]]; then
  # exist
  echo -e "\nTrigger $TRIGGER_NAME does EXIST, updating"
  aws glue update-trigger \
    --name $TRIGGER_NAME \
    --trigger-update '{"Name":"'$TRIGGER_NAME'","Schedule":"'"${TRIGGER_CRONJOB}"'","Actions":[{"JobName":"'$JOB_NAME'","Arguments":{"--IS_ATHENA":"'$IS_ATHENA'","--IS_LOCAL":"'$IS_LOCAL'","--env":"'$TARGET_ENV'"},"Timeout":45}]}'
  echo -e "\nTrigger $TRIGGER_NAME UPDATED"

else
  echo -e "\nTrigger $TRIGGER_NAME does NOT exist, creating ..."
  aws glue create-trigger \
    --name $TRIGGER_NAME \
    --type $TRIGGER_TYPE \
    --schedule "$TRIGGER_CRONJOB" \
    --actions '[{"JobName":"'$JOB_NAME'","Arguments":{"--IS_ATHENA":"'$IS_ATHENA'","--IS_LOCAL":"'$IS_LOCAL'","--env":"'$TARGET_ENV'"},"Timeout":45}]'
  echo -e "\nTrigger $TRIGGER_NAME CREATED"
fi

aws glue start-trigger --name $TRIGGER_NAME
echo -e "\nTrigger $TRIGGER_NAME ACTIVATED"
