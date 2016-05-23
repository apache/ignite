#!/bin/sh

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#profile=/home/ignite/.bash_profile
profile=/root/.bash_profile

. $profile
. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "ignite"

setupCassandraSeeds()
{
    setupClusterSeeds "cassandra" "true"

    CLUSTER_SEEDS=($CLUSTER_SEEDS)
	count=${#CLUSTER_SEEDS[@]}

    CASSANDRA_SEEDS=

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		seed=${CLUSTER_SEEDS[$i]}
        CASSANDRA_SEEDS="${CASSANDRA_SEEDS}<value>$seed<\/value>"
	done

    cat /opt/ignite/config/ignite-cassandra-server-template.xml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS/g" > /opt/ignite/config/ignite-cassandra-server.xml
}

setupIgniteSeeds()
{
    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        echo "[INFO] Setting up Ignite seeds"

        CLUSTER_SEEDS="127.0.0.1:47500..47509"

        echo "[INFO] Using localhost address as a seed for the first Ignite node: $CLUSTER_SEEDS"

        aws s3 rm --recursive ${S3_IGNITE_NODES_DISCOVERY::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY"
        fi
    else
        setupClusterSeeds "ignite" "true"
    fi

    CLUSTER_SEEDS=($CLUSTER_SEEDS)
	count=${#CLUSTER_SEEDS[@]}

    IGNITE_SEEDS=

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		seed=${CLUSTER_SEEDS[$i]}
        IGNITE_SEEDS="${IGNITE_SEEDS}<value>$seed<\/value>"
	done

    cat /opt/ignite/config/ignite-cassandra-server.xml | sed -r "s/\\\$\{IGNITE_SEEDS\}/$IGNITE_SEEDS/g" > /opt/ignite/config/ignite-cassandra-server1.xml
    mv -f /opt/ignite/config/ignite-cassandra-server1.xml /opt/ignite/config/ignite-cassandra-server.xml
}

checkIgniteStatus()
{
    proc=$(ps -ef | grep java | grep "org.apache.ignite.startup.cmdline.CommandLineStartup")

    nodeId=
    nodeAddrs=
    nodePorts=
    topology=
    metrics=

    logFile=$(ls /opt/ignite/work/log/ | grep "\.log$")
    if [ -n "$logFile" ]; then
        logFile=/opt/ignite/work/log/$logFile
        nodeId=$(cat $logFile | grep "Local node \[ID")
        nodeAddrs=$(cat $logFile | grep "Local node addresses:")
        nodePorts=$(cat $logFile | grep "Local ports:")
        topology=$(cat $logFile | grep "Topology snapshot")
        metrics=$(cat $logFile | grep "Metrics for local node" | head -n 1)
    fi

    if [ -n "$nodeId" ] && [ -n "$nodeAddrs" ] && [ -n "$nodePorts" ] && [ -n "$topology" ] && [ -n "$metrics" ] && [ -n "$proc" ]; then
        sleep 30s
        return 0
    fi

    return 1
}

startIgnite()
{
    echo "[INFO]-------------------------------------------------------------"
    echo "[INFO] Trying attempt $START_ATTEMPT to start Ignite daemon"
    echo "[INFO]-------------------------------------------------------------"
    echo ""

    setupCassandraSeeds
    setupIgniteSeeds

    waitToJoinCluster

    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        aws s3 rm --recursive ${S3_IGNITE_NODES_DISCOVERY::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY"
        fi
    fi

    proc=$(ps -ef | grep java | grep "org.apache.ignite.startup.cmdline.CommandLineStartup")
    proc=($proc)

    if [ -n "${proc[1]}" ]; then
        echo "[INFO] Terminating existing Ignite process ${proc[1]}"
        kill -9 ${proc[1]}
    fi

    echo "[INFO] Starting Ignite"
    rm -Rf /opt/ignite/work/*
    /opt/ignite/bin/ignite.sh /opt/ignite/config/ignite-cassandra-server.xml &

    echo "[INFO] Ignite job id: $!"

    sleep 1m

    START_ATTEMPT=$(( $START_ATTEMPT+1 ))
}

#######################################################################################################

START_ATTEMPT=0

unregisterNode

tryToGetFirstNodeLock

echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE_LOCK" == "true" ]; then
    echo "[INFO] Starting first Ignite node"
else
    echo "[INFO] Starting Ignite node"
fi

echo "[INFO]-----------------------------------------------------------------"
printInstanceInfo
echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE_LOCK" != "true" ]; then
    waitFirstClusterNodeRegistered "true"
else
    cleanupMetadata
fi

envScript=$(readlink -m $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/ignite-env.sh)
if [ -f "$envScript" ]; then
    . $envScript
fi

startIgnite

startTime=$(date +%s)

while true; do
    proc=$(ps -ef | grep java | grep "org.apache.ignite.startup.cmdline.CommandLineStartup")

    checkIgniteStatus

    if [ $? -eq 0 ]; then
        sleep 1m
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Ignite daemon successfully started"
        echo "[INFO]-----------------------------------------------------"
        echo $proc
        echo "[INFO]-----------------------------------------------------"

        removeClusterJoinLock

        break
    fi

    currentTime=$(date +%s)
    duration=$(( $currentTime-$startTime ))
    duration=$(( $duration/60 ))

    if [ $duration -gt $SERVICE_STARTUP_TIME ]; then
        if [ "$FIRST_NODE_LOCK" == "true" ]; then
            terminate "${SERVICE_STARTUP_TIME}min timeout expired, but first Ignite daemon is still not up and running"
        else
            removeClusterJoinLock

            if [ $START_ATTEMPT -gt $SERVICE_START_ATTEMPTS ]; then
                terminate "${SERVICE_START_ATTEMPTS} attempts exceed, but Ignite daemon is still not up and running"
            fi

            startIgnite
        fi

        continue
    fi

    if [ -z "$proc" ]; then
        if [ "$FIRST_NODE_LOCK" == "true" ]; then
            terminate "Failed to start Ignite daemon"
        fi

        removeClusterJoinLock
        echo "[WARN] Failed to start Ignite daemon. Sleeping for extra 30sec"
        sleep 30s
        startIgnite
        continue
    fi

    echo "[INFO] Waiting for Ignite daemon to start, time passed ${duration}min"
    sleep 30s
done

registerNode

terminate