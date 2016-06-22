#!/bin/bash

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

terminate()
{
    if [[ "$S3_BOOTSTRAP_SUCCESS_URL" != */ ]]; then
        S3_BOOTSTRAP_SUCCESS_URL=${S3_BOOTSTRAP_SUCCESS_URL}/
    fi

    if [[ "$S3_BOOTSTRAP_FAILURE_URL" != */ ]]; then
        S3_BOOTSTRAP_FAILURE_URL=${S3_BOOTSTRAP_FAILURE_URL}/
    fi

    msg=$HOST_NAME

    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Failed to start Cassandra node"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1
        reportFolder=${S3_BOOTSTRAP_FAILURE_URL}${HOST_NAME}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Cassandra node successfully started"
        echo "[INFO]-----------------------------------------------------"
        reportFolder=${S3_BOOTSTRAP_SUCCESS_URL}${HOST_NAME}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/cassandra/start_result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed drop report folder: $reportFolder"
    fi

    if [ -d "/opt/cassandra/logs" ]; then
        aws s3 sync --sse AES256 /opt/cassandra/logs $reportFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export Cassandra logs to: $reportFolder"
        fi
    fi

    aws s3 cp --sse AES256 /opt/cassandra/start_result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to export node start result to: $reportFile"
    fi

    rm -f /opt/cassandra/start_result /opt/cassandra/join-lock /opt/cassandra/remote-join-lock

    if [ -n "$1" ]; then
        exit 1
    fi

    exit 0
}

registerNode()
{
    echo "[INFO] Registering Cassandra node seed: ${S3_CASSANDRA_NODES_DISCOVERY_URL}$HOST_NAME"

    aws s3 cp --sse AES256 /opt/cassandra/join-lock ${S3_CASSANDRA_NODES_DISCOVERY_URL}$HOST_NAME
    if [ $? -ne 0 ]; then
        terminate "Failed to register Cassandra seed info in: ${S3_CASSANDRA_NODES_DISCOVERY_URL}$HOST_NAME"
    fi

    echo "[INFO] Cassandra node seed successfully registered"
}

unregisterNode()
{
    echo "[INFO] Removing Cassandra node registration from: ${S3_CASSANDRA_NODES_DISCOVERY_URL}$HOST_NAME"
    aws s3 rm ${S3_CASSANDRA_NODES_DISCOVERY_URL}$HOST_NAME
    echo "[INFO] Cassandra node registration removed"
}

cleanupMetadata()
{
    echo "[INFO] Running cleanup"
    aws s3 rm $S3_CASSANDRA_NODES_JOIN_LOCK_URL
    aws s3 rm --recursive $S3_CASSANDRA_NODES_DISCOVERY_URL
    aws s3 rm --recursive $S3_BOOTSTRAP_SUCCESS_URL
    aws s3 rm --recursive $S3_BOOTSTRAP_FAILURE_URL
    echo "[INFO] Cleanup completed"
}

setupCassandraSeeds()
{
    echo "[INFO] Setting up Cassandra seeds"

    if [ "$FIRST_NODE" == "true" ]; then
        CASSANDRA_SEEDS=$(hostname -f | tr '[:upper:]' '[:lower:]')
        echo "[INFO] Using host address as a seed for the first Cassandra node: $CASSANDRA_SEEDS"
        aws s3 rm --recursive ${S3_CASSANDRA_NODES_DISCOVERY_URL::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
        fi

        cat /opt/cassandra/conf/cassandra-template.yaml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS/g" > /opt/cassandra/conf/cassandra.yaml

        return 0
    fi

    echo "[INFO] Looking for Cassandra seeds in: $S3_CASSANDRA_NODES_DISCOVERY_URL"

    startTime=$(date +%s)

    while true; do
        seeds=$(aws s3 ls $S3_CASSANDRA_NODES_DISCOVERY_URL | grep -v PRE | sed -r "s/^.* //g")
        if [ -n "$seeds" ]; then
            seeds=($seeds)
            length=${#seeds[@]}

            if [ $length -lt 4 ]; then
                seed1=${seeds[0]}
                seed2=${seeds[1]}
                seed3=${seeds[2]}
            else
                pos1=$(($RANDOM%$length))
                pos2=$(($RANDOM%$length))
                pos3=$(($RANDOM%$length))
                seed1=${seeds[${pos1}]}
                seed2=${seeds[${pos2}]}
                seed3=${seeds[${pos3}]}
            fi

            CASSANDRA_SEEDS=$seed1
            CASSANDRA_SEED=$seed1

            if [ "$seed2" != "$seed1" ] && [ -n "$seed2" ]; then
                CASSANDRA_SEEDS="$CASSANDRA_SEEDS,$seed2"
            fi

            if [ "$seed3" != "$seed2" ] && [ "$seed3" != "$seed1" ] && [ -n "$seed3" ]; then
                CASSANDRA_SEEDS="$CASSANDRA_SEEDS,$seed3"
            fi

            echo "[INFO] Using Cassandra seeds: $CASSANDRA_SEEDS"

            cat /opt/cassandra/conf/cassandra-template.yaml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS/g" > /opt/cassandra/conf/cassandra.yaml

            return 0
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ $duration -gt $NODE_STARTUP_TIME ]; then
            terminate "${NODE_STARTUP_TIME}min timeout expired, but first Cassandra node is still not up and running"
        fi

        echo "[INFO] Waiting for the first Cassandra node to start and publish its seed, time passed ${duration}min"

        sleep 1m
    done
}

tryToGetFirstNodeLock()
{
    echo "[INFO] Trying to get first node lock"

    checkFirstNodeLockExist
    if [ $? -ne 0 ]; then
        return 1
    fi

    createFirstNodeLock

    sleep 5s

    rm -Rf /opt/cassandra/first-node-lock

    aws s3 cp $S3_CASSANDRA_FIRST_NODE_LOCK_URL /opt/cassandra/first-node-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created first node lock"
        return 1
    fi

    first_host=$(cat /opt/cassandra/first-node-lock)

    rm -f /opt/cassandra/first-node-lock

    if [ "$first_host" != "$HOST_NAME" ]; then
        echo "[INFO] Node $first_host has discarded previously created first node lock"
        return 1
    fi

    echo "[INFO] Congratulations, got first node lock"

    return 0
}

checkFirstNodeLockExist()
{
    echo "[INFO] Checking for the first node lock"

    lockExists=$(aws s3 ls $S3_CASSANDRA_FIRST_NODE_LOCK_URL)
    if [ -n "$lockExists" ]; then
        echo "[INFO] First node lock already exists"
        return 1
    fi

    echo "[INFO] First node lock doesn't exist"

    return 0
}

createFirstNodeLock()
{
    aws s3 cp --sse AES256 /opt/cassandra/join-lock $S3_CASSANDRA_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to create first node lock"
    fi
    echo "[INFO] Created first node lock"
}

removeFirstNodeLock()
{
    aws s3 rm $S3_CASSANDRA_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to remove first node lock"
    fi
    echo "[INFO] Removed first node lock"
}

tryToGetClusterJoinLock()
{
    echo "[INFO] Trying to get cluster join lock"

    checkClusterJoinLockExist
    if [ $? -ne 0 ]; then
        return 1
    fi

    createClusterJoinLock

    sleep 5s

    rm -Rf /opt/cassandra/remote-join-lock

    aws s3 cp $S3_CASSANDRA_NODES_JOIN_LOCK_URL /opt/cassandra/remote-join-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created cluster join lock"
        return 1
    fi

    join_host=$(cat /opt/cassandra/remote-join-lock)

    if [ "$join_host" != "$HOST_NAME" ]; then
        echo "[INFO] Node $first_host has discarded previously created cluster join lock"
        return 1
    fi

    echo "[INFO] Congratulations, got cluster join lock"

    return 0
}

checkClusterJoinLockExist()
{
    echo "[INFO] Checking for the cluster join lock"

    lockExists=$(aws s3 ls $S3_CASSANDRA_NODES_JOIN_LOCK_URL)
    if [ -n "$lockExists" ]; then
        echo "[INFO] Cluster join lock already exists"
        return 1
    fi

    status=$(/opt/cassandra/bin/nodetool -h $CASSANDRA_SEED status)
    leaving=$(echo $status | grep UL)
    moving=$(echo $status | grep UM)
    joining=$(echo $status | grep UJ)

    if [ -n "$leaving" ] || [ -n "$moving" ] || [ -n "$joining" ]; then
        echo "[INFO] Cluster join lock doesn't exist in S3, but some node still trying to join Cassandra cluster"
        return 1
    fi

    echo "[INFO] Cluster join lock doesn't exist"

    return 0
}

createClusterJoinLock()
{
    aws s3 cp --sse AES256 /opt/cassandra/join-lock $S3_CASSANDRA_NODES_JOIN_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to create cluster join lock"
    fi
    echo "[INFO] Created cluster join lock"
}

removeClusterJoinLock()
{
    aws s3 rm $S3_CASSANDRA_NODES_JOIN_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to remove cluster join lock"
    fi
    echo "[INFO] Removed cluster join lock"
}

waitToJoinCassandraCluster()
{
    echo "[INFO] Waiting to join Cassandra cluster"

    while true; do
        tryToGetClusterJoinLock

        if [ $? -ne 0 ]; then
            echo "[INFO] Another node is trying to join cluster. Waiting for extra 1min."
            sleep 1m
        else
            echo "[INFO]-------------------------------------------------------------"
            echo "[INFO] Congratulations, got lock to join Cassandra cluster"
            echo "[INFO]-------------------------------------------------------------"
            break
        fi
    done
}

waitFirstCassandraNodeRegistered()
{
    echo "[INFO] Waiting for the first Cassandra node to register"

    startTime=$(date +%s)

    while true; do
        first_host=

        exists=$(aws s3 ls $S3_CASSANDRA_FIRST_NODE_LOCK_URL)
        if [ -n "$exists" ]; then
            rm -Rf /opt/cassandra/first-node-lock

            aws s3 cp $S3_CASSANDRA_FIRST_NODE_LOCK_URL /opt/cassandra/first-node-lock
            if [ $? -ne 0 ]; then
                terminate "Failed to check existing first node lock"
            fi

            first_host=$(cat /opt/cassandra/first-node-lock)

            rm -Rf /opt/cassandra/first-node-lock
        fi

        if [ -n "$first_host" ]; then
            exists=$(aws s3 ls ${S3_CASSANDRA_NODES_DISCOVERY_URL}${first_host})
            if [ -n "$exists" ]; then
                break
            fi
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ $duration -gt $NODE_STARTUP_TIME ]; then
            terminate "${NODE_STARTUP_TIME}min timeout expired, but first Cassandra node is still not up and running"
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] First Cassandra node registered"
}

startCassandra()
{
    echo "[INFO]-------------------------------------------------------------"
    echo "[INFO] Trying attempt $START_ATTEMPT to start Cassandra daemon"
    echo "[INFO]-------------------------------------------------------------"
    echo ""

    setupCassandraSeeds

    if [ "$FIRST_NODE" != "true" ]; then
        waitToJoinCassandraCluster
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

# Time (in minutes) to wait for the Cassandra node up and running and register it in S3
NODE_STARTUP_TIME=10

# Number of attempts to start (not first) Cassandra daemon
NODE_START_ATTEMPTS=3

HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
echo $HOST_NAME > /opt/cassandra/join-lock

START_ATTEMPT=0

FIRST_NODE="false"

unregisterNode

tryToGetFirstNodeLock

if [ $? -eq 0 ]; then
    FIRST_NODE="true"
fi

echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE" == "true" ]; then
    echo "[INFO] Starting first Cassandra node"
else
    echo "[INFO] Starting Cassandra node"
fi

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Cassandra nodes discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
echo "[INFO] Cassandra first node lock URL: $S3_CASSANDRA_FIRST_NODE_LOCK_URL"
echo "[INFO] Cassandra nodes join lock URL: $S3_CASSANDRA_NODES_JOIN_LOCK_URL"
echo "[INFO] Start success URL: $S3_BOOTSTRAP_SUCCESS_URL"
echo "[INFO] Start failure URL: $S3_BOOTSTRAP_FAILURE_URL"
echo "[INFO] CASSANDRA_HOME: $CASSANDRA_HOME"
echo "[INFO] JAVA_HOME: $JAVA_HOME"
echo "[INFO] PATH: $PATH"
echo "[INFO]-----------------------------------------------------------------"

if [ -z "$S3_CASSANDRA_NODES_DISCOVERY_URL" ]; then
    terminate "S3 discovery URL doesn't specified"
fi

if [[ "$S3_CASSANDRA_NODES_DISCOVERY_URL" != */ ]]; then
    S3_CASSANDRA_NODES_DISCOVERY_URL=${S3_CASSANDRA_NODES_DISCOVERY_URL}/
fi

if [ "$FIRST_NODE" != "true" ]; then
    waitFirstCassandraNodeRegistered
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

        if [ "$FIRST_NODE" != "true" ]; then
            removeClusterJoinLock
        fi

        break
    fi

    currentTime=$(date +%s)
    duration=$(( $currentTime-$startTime ))
    duration=$(( $duration/60 ))

    if [ $duration -gt $NODE_STARTUP_TIME ]; then
        if [ "$FIRST_NODE" == "true" ]; then
            removeFirstNodeLock
            terminate "${NODE_STARTUP_TIME}min timeout expired, but first Cassandra daemon is still not up and running"
        else
            removeClusterJoinLock

            if [ $START_ATTEMPT -gt $NODE_START_ATTEMPTS ]; then
                terminate "${NODE_START_ATTEMPTS} attempts exceed, but Cassandra daemon is still not up and running"
            fi

            startCassandra
        fi

        continue
    fi

    concurrencyError=$(cat /opt/cassandra/logs/system.log | grep "java.lang.UnsupportedOperationException: Other bootstrapping/leaving/moving nodes detected, cannot bootstrap while cassandra.consistent.rangemovement is true")

    if [ -n "$concurrencyError" ] && [ "$FIRST_NODE" != "true" ]; then
        removeClusterJoinLock
        echo "[WARN] Failed to concurrently start Cassandra daemon. Sleeping for extra 1min"
        sleep 1m
        startCassandra
        continue
    fi

    if [ -z "$proc" ]; then
        if [ "$FIRST_NODE" == "true" ]; then
            removeFirstNodeLock
            terminate "Failed to start Cassandra daemon"
        fi

        removeClusterJoinLock
        echo "[WARN] Failed to start Cassandra daemon. Sleeping for extra 1min"
        sleep 1m
        startCassandra
        continue
    fi

    echo "[INFO] Waiting for Cassandra daemon to start, time passed ${duration}min"
    sleep 30s
done

registerNode

terminate