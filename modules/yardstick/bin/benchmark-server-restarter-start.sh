#!/bin/bash

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

#
# Script that starts server restarter for given host name.
# This script expects the arguments to be
# - server host name to be restarted
# - config to start new instances of server
# - warmup delay
# - restart period
# - a path to run properties file which contains the list of remote nodes to start server on
#

# Define script directory.
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

HOST_NAME=$1
ID=$2
CONFIG=$3
WARMPUP_DELAY=$4
PAUSE=$5
PERIOD=$6
CONFIG_INCLUDE=$7

if [ "${CONFIG_INCLUDE}" == "-h" ] || [ "${CONFIG_INCLUDE}" == "--help" ]; then
    echo "Usage: benchmark-server-restarter-start.sh [HOST_NAME] [ID] [CONFIG] [DELAY] [PAUSE] [PERIOD] [PROPERTIES_FILE_PATH]"
    echo "Script that starts server restarter according to given paramethers."
    exit 1
fi

if [ "${CONFIG_INCLUDE}" == "" ]; then
    CONFIG_INCLUDE=${SCRIPT_DIR}/../config/benchmark.properties
    echo "<"$(date +"%H:%M:%S")"><yardstick> Using default properties file: config/benchmark.properties"
fi

if [ ! -f $CONFIG_INCLUDE ]; then
    echo "ERROR: Properties file is not found."
    echo "Type \"--help\" for usage."
    exit 1
fi

shift

CONFIG_TMP=`mktemp tmp.XXXXXXXX`

cp $CONFIG_INCLUDE $CONFIG_TMP
chmod +x $CONFIG_TMP

. $CONFIG_TMP
rm $CONFIG_TMP

# Define user to establish remote ssh session.
if [ "${REMOTE_USER}" == "" ]; then
    REMOTE_USER=$(whoami)
fi

if [ "${SERVER_HOSTS}" == "" ]; then
    echo "ERROR: Benchmark hosts (SERVER_HOSTS) is not defined in properties file."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${REMOTE_USER}" == "" ]; then
    echo "ERROR: Remote user (REMOTE_USER) is not defined in properties file."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${CONFIG}" == "" ]; then
    echo "ERROR: Config (CONFIG) is not defined. Restarter should be run with specified config."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${HOST_NAME}" == "" ]; then
    echo "ERROR: HOST_NAME are not defined."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${WARMPUP_DELAY}" == "" ]; then
    echo "ERROR: warmup_delay are not defined."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${PERIOD}" == "" ]; then
    echo "ERROR: PERIOD are not defined."
    echo "Type \"--help\" for usage."
    exit 1
fi

if [ "${SERVERS_LOGS_DIR}" = "" ]; then
    SERVERS_LOGS_DIR=${SCRIPT_DIR}/../${LOGS_BASE}/logs_servers
fi

CUR_DIR=$(pwd)

echo "<"$(date +"%H:%M:%S")"><yardstick> Server restarer started for ${HOST_NAME} with id=${ID}."

#
# Main.
#
ssh -o PasswordAuthentication=no ${REMOTE_USER}"@"${HOST_NAME} mkdir -p ${SERVERS_LOGS_DIR}

DS=""

# Extract description.
if [[ "${RESTART_SERVERS}" != "" ]]; then
    IFS=' ' read -ra cfg0 <<< "${CONFIG}"
    for cfg00 in "${cfg0[@]}";
    do
        if [[ ${found} == 'true' ]]; then
            found=""
            DS=${cfg00}
        fi

        if [[ ${cfg00} == '-ds' ]] || [[ ${cfg00} == '--descriptions' ]]; then
            found="true"
        fi
    done
fi

cntr=1

sleep ${WARMPUP_DELAY}

while [ true ]
do
    echo "<"$(date +"%H:%M:%S")"><yardstick> Killing server on "${HOST_NAME}" with id=${ID}"

    # Kill only first found yardstick.server on the host
    ssh -o PasswordAuthentication=no ${REMOTE_USER}"@"${HOST_NAME} "pkill -9 -f 'Dyardstick.server${ID}'"

    sleep ${PAUSE} # Wait for process stopping.

    suffix=`echo "${CONFIG}" | tail -c 60 | sed 's/ *$//g'`

    echo "<"$(date +"%H:%M:%S")"><yardstick> Starting server config '...${suffix}' on ${HOST_NAME} with id=${ID}"

    now=`date +'%H%M%S'`

    server_file_log=${SERVERS_LOGS_DIR}"/"${now}"_id"${ID}"-"${cntr}"_"${HOST_NAME}${DS}".log"

    ssh -o PasswordAuthentication=no ${REMOTE_USER}"@"${HOST_NAME} \
        "MAIN_CLASS='org.yardstickframework.BenchmarkServerStartUp'" \
        "JVM_OPTS='${JVM_OPTS} -Dyardstick.server${ID}-${cntr}'" "CP='${CP}'" "CUR_DIR='${CUR_DIR}'" "PROPS_ENV0='${PROPS_ENV}'" \
        "nohup ${SCRIPT_DIR}/benchmark-bootstrap.sh ${CONFIG} "--config" ${CONFIG_INCLUDE} > ${server_file_log} 2>& 1 &"

    echo "<"$(date +"%H:%M:%S")"><yardstick> Server on ${HOST_NAME} with id=${ID} was started."

    cntr=$((1 + $cntr))

    sleep ${PERIOD}
done
