#!/bin/bash
#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
#

#
# Grid command line loader.
#

# Define environment paths.
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

export CONFIG_DIR=$SCRIPT_DIR/../resources
export CLIENTS_MODULE_PATH=$SCRIPT_DIR/../../..
export BIN_PATH=$SCRIPT_DIR/../../../../../bin

IGNITE_HOME=$(cd $SCRIPT_DIR/../../../../../..; pwd)

if [ ! -d "${JAVA_HOME}" ]; then
    JAVA_HOME=/Library/Java/Home
fi

export JAVA_HOME
export IGNITE_HOME

echo Switch to home directory $IGNITE_HOME
cd $IGNITE_HOME

MVN_EXEC=mvn

if [ -d "${M2_HOME}" ]; then
    MVN_EXEC=${M2_HOME}/bin/${MVN_EXEC}
fi

${MVN_EXEC} -P+test,-scala,-examples,-release clean package -DskipTests -DskipClientDocs

echo Switch to build script directory $SCRIPT_DIR
cd $SCRIPT_DIR

if [ ! -d "${IGNITE_HOME}/work" ]; then
    mkdir "${IGNITE_HOME}/work"
fi

if [ ! -d "${IGNITE_HOME}/work/log" ]; then
    mkdir "${IGNITE_HOME}/work/log"
fi

export JVM_OPTS="${JVM_OPTS} -DCLIENTS_MODULE_PATH=${CLIENTS_MODULE_PATH}"

for iter in {1..2}
do
    LOG_FILE=${IGNITE_HOME}/work/log/node-${iter}.log

    echo Start node ${iter}: ${LOG_FILE}
    nohup /bin/bash $BIN_PATH/ignite.sh $CONFIG_DIR/spring-server-node.xml -v < /dev/null > ${LOG_FILE} 2>&1 &
done

for iter in {1..2}
do
    LOG_FILE=${IGNITE_HOME}/work/log/node-ssl-${iter}.log

    echo Start SSL node ${iter}: ${LOG_FILE}
    nohup /bin/bash $BIN_PATH/ignite.sh $CONFIG_DIR/spring-server-ssl-node.xml -v < /dev/null > ${LOG_FILE} 2>&1 &
done

echo Wait 60 seconds while nodes start.
sleep 60

LOG_FILE=${IGNITE_HOME}/work/log/router.log
echo Start Router: ${LOG_FILE}
nohup /bin/bash $BIN_PATH/igniterouter.sh $CONFIG_DIR/spring-router.xml -v < /dev/null > ${LOG_FILE} 2>&1 &

# Disable hostname verification for self-signed certificates.
export JVM_OPTS="${JVM_OPTS} -DIGNITE_DISABLE_HOSTNAME_VERIFIER=true"

LOG_FILE=${IGNITE_HOME}/work/log/router-ssl.log
echo Start Router SSL: ${LOG_FILE}
nohup /bin/bash $BIN_PATH/igniterouter.sh $CONFIG_DIR/spring-router-ssl.xml -v < /dev/null > ${LOG_FILE} 2>&1 &

echo Wait 30 seconds while router starts.
sleep 30

echo
echo Expect all nodes are started.
