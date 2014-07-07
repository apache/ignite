#!/bin/bash
#
# @sh.file.header
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: @sh.file.version
#

#
# Grid command line loader.
#

# Define environment paths.
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

export CONFIG_DIR=$SCRIPT_DIR/../resources
export CLIENTS_MODULE_PATH=$SCRIPT_DIR/../../..
export BIN_PATH=$SCRIPT_DIR/../../../../../bin

GG_HOME=$(cd $SCRIPT_DIR/../../../../../..; pwd)

if [ ! -d "${JAVA_HOME}" ]; then
    JAVA_HOME=/Library/Java/Home
fi

export JAVA_HOME
export GG_HOME

echo Switch to home directory $GG_HOME
cd $GG_HOME

MVN_EXEC=mvn

if [ -d "${M2_HOME}" ]; then
    MVN_EXEC=${M2_HOME}/bin/${MVN_EXEC}
fi

${MVN_EXEC} -P+test,-scala,-examples,-release clean package -DskipTests -DskipClientDocs

echo Switch to build script directory $SCRIPT_DIR
cd $SCRIPT_DIR

if [ ! -d "${GG_HOME}/work" ]; then
    mkdir "${GG_HOME}/work"
fi

if [ ! -d "${GG_HOME}/work/log" ]; then
    mkdir "${GG_HOME}/work/log"
fi

export JVM_OPTS="${JVM_OPTS} -DCLIENTS_MODULE_PATH=${CLIENTS_MODULE_PATH}"

for iter in {1..2}
do
    LOG_FILE=${GG_HOME}/work/log/node-${iter}.log

    echo Start node ${iter}: ${LOG_FILE}
    nohup /bin/bash $BIN_PATH/ggstart.sh $CONFIG_DIR/spring-server-node.xml -v < /dev/null > ${LOG_FILE} 2>&1 &
done

for iter in {1..2}
do
    LOG_FILE=${GG_HOME}/work/log/node-ssl-${iter}.log

    echo Start SSL node ${iter}: ${LOG_FILE}
    nohup /bin/bash $BIN_PATH/ggstart.sh $CONFIG_DIR/spring-server-ssl-node.xml -v < /dev/null > ${LOG_FILE} 2>&1 &
done

echo Wait 60 seconds while nodes start.
sleep 60

LOG_FILE=${GG_HOME}/work/log/router.log
echo Start Router: ${LOG_FILE}
nohup /bin/bash $BIN_PATH/ggrouter.sh $CONFIG_DIR/spring-router.xml -v < /dev/null > ${LOG_FILE} 2>&1 &

# Disable hostname verification for self-signed certificates.
export JVM_OPTS="${JVM_OPTS} -DGRIDGAIN_DISABLE_HOSTNAME_VERIFIER=true"

LOG_FILE=${GG_HOME}/work/log/router-ssl.log
echo Start Router SSL: ${LOG_FILE}
nohup /bin/bash $BIN_PATH/ggrouter.sh $CONFIG_DIR/spring-router-ssl.xml -v < /dev/null > ${LOG_FILE} 2>&1 &

echo Wait 30 seconds while router starts.
sleep 30

echo
echo Expect all nodes are started.
