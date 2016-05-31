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

#profile=/home/cassandra/.bash_profile
profile=/root/.bash_profile

. $profile
. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "cassandra"

setupCassandraSeeds()
{
    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        echo "[INFO] Setting up Cassandra seeds"

        CLUSTER_SEEDS=$(hostname -f | tr '[:upper:]' '[:lower:]')

        echo "[INFO] Using host address as a seed for the first Cassandra node: $CLUSTER_SEEDS"

        aws s3 rm --recursive ${S3_CASSANDRA_NODES_DISCOVERY::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY"
        fi
    else
        setupClusterSeeds "cassandra" "true"
        CLUSTER_SEEDS=$(echo $CLUSTER_SEEDS | sed -r "s/ /,/g")
    fi

    cat /opt/cassandra/conf/cassandra-template.yaml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CLUSTER_SEEDS/g" > /opt/cassandra/conf/cassandra.yaml
}

startCassandra()
{
    echo "[INFO]-------------------------------------------------------------"
    echo "[INFO] Trying attempt $START_ATTEMPT to start Cassandra daemon"
    echo "[INFO]-------------------------------------------------------------"
    echo ""

    setupCassandraSeeds

    waitToJoinCluster

    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        aws s3 rm --recursive ${S3_CASSANDRA_NODES_DISCOVERY::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Cassandra node discovery URL: $S3_IGNITE_NODES_DISCOVERY"
        fi
    fi

    proc=$(ps -ef | grep java | grep "org.apache.cassandra.service.CassandraDaemon")
    proc=($proc)

    if [ -n "${proc[1]}" ]; then
        echo "[INFO] Terminating existing Cassandra process ${proc[1]}"
        kill -9 ${proc[1]}
    fi

    echo "[INFO] Starting Cassandra"
    rm -Rf /opt/cassandra/logs/* /storage/cassandra/*
    /opt/cassandra/bin/cassandra -R &

    echo "[INFO] Cassandra job id: $!"

    sleep 1m

    START_ATTEMPT=$(( $START_ATTEMPT+1 ))
}

#######################################################################################################

START_ATTEMPT=0

unregisterNode

tryToGetFirstNodeLock

echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE_LOCK" == "true" ]; then
    echo "[INFO] Starting first Cassandra node"
else
    echo "[INFO] Starting Cassandra node"
fi

echo "[INFO]-----------------------------------------------------------------"
printInstanceInfo
echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE_LOCK" != "true" ]; then
    waitFirstClusterNodeRegistered "true"
else
    cleanupMetadata
fi

startCassandra

startTime=$(date +%s)

while true; do
    proc=$(ps -ef | grep java | grep "org.apache.cassandra.service.CassandraDaemon")

    /opt/cassandra/bin/nodetool status &> /dev/null

    if [ $? -eq 0 ]; then
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Cassandra daemon successfully started"
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
            terminate "${SERVICE_STARTUP_TIME}min timeout expired, but first Cassandra daemon is still not up and running"
        else
            removeClusterJoinLock

            if [ $START_ATTEMPT -gt $SERVICE_START_ATTEMPTS ]; then
                terminate "${SERVICE_START_ATTEMPTS} attempts exceed, but Cassandra daemon is still not up and running"
            fi

            startCassandra
        fi

        continue
    fi

    concurrencyError=$(cat /opt/cassandra/logs/system.log | grep "java.lang.UnsupportedOperationException: Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true")

    if [ -n "$concurrencyError" ] && [ "$FIRST_NODE_LOCK" != "true" ]; then
        removeClusterJoinLock
        echo "[WARN] Failed to concurrently start Cassandra daemon. Sleeping for extra 30sec"
        sleep 30s
        startCassandra
        continue
    fi

    if [ -z "$proc" ]; then
        if [ "$FIRST_NODE_LOCK" == "true" ]; then
            terminate "Failed to start Cassandra daemon"
        fi

        removeClusterJoinLock
        echo "[WARN] Failed to start Cassandra daemon. Sleeping for extra 30sec"
        sleep 30s
        startCassandra
        continue
    fi

    echo "[INFO] Waiting for Cassandra daemon to start, time passed ${duration}min"
    sleep 30s
done

registerNode

terminate