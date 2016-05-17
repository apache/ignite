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

#profile=/home/ignite/.bash_profile
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
        echo "[ERROR] Failed to start Ignite node"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1
        reportFolder=${S3_BOOTSTRAP_FAILURE_URL}${HOST_NAME}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Ignite node successfully started"
        echo "[INFO]-----------------------------------------------------"
        reportFolder=${S3_BOOTSTRAP_SUCCESS_URL}${HOST_NAME}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/ignite/start_result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed drop report folder: $reportFolder"
    fi

    if [ -d "/opt/ignite/work/log" ]; then
        aws s3 sync --sse AES256 /opt/ignite/work/log $reportFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export Ignite logs to: $reportFolder"
        fi
    fi

    aws s3 cp --sse AES256 /opt/ignite/start_result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to export node start result to: $reportFile"
    fi

    rm -f /opt/ignite/start_result /opt/ignite/join-lock /opt/ignite/remote-join-lock

    if [ -n "$1" ]; then
        exit 1
    fi

    exit 0
}

registerNode()
{
    echo "[INFO] Registering Ignite node seed: ${S3_IGNITE_NODES_DISCOVERY_URL}$HOST_NAME"

    aws s3 cp --sse AES256 /opt/ignite/join-lock ${S3_IGNITE_NODES_DISCOVERY_URL}$HOST_NAME
    if [ $? -ne 0 ]; then
        terminate "Failed to register Ignite node seed: ${S3_IGNITE_NODES_DISCOVERY_URL}$HOST_NAME"
    fi

    echo "[INFO] Ignite node seed successfully registered"
}

unregisterNode()
{
    echo "[INFO] Removing Ignite node registration from: ${S3_IGNITE_NODES_DISCOVERY_URL}$HOST_NAME"
    aws s3 rm ${S3_IGNITE_NODES_DISCOVERY_URL}$HOST_NAME
    echo "[INFO] Ignite node registration removed"
}

cleanupMetadata()
{
    echo "[INFO] Running cleanup"
    aws s3 rm $S3_IGNITE_NODES_JOIN_LOCK_URL
    aws s3 rm --recursive $S3_IGNITE_NODES_DISCOVERY_URL
    aws s3 rm --recursive $S3_BOOTSTRAP_SUCCESS_URL
    aws s3 rm --recursive $S3_BOOTSTRAP_FAILURE_URL
    echo "[INFO] Cleanup completed"
}

setupCassandraSeeds()
{
    echo "[INFO] Setting up Cassandra seeds"

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

            CASSANDRA_SEEDS="<value>$seed1<\/value>"

            if [ "$seed2" != "$seed1" ] && [ -n "$seed2" ]; then
                CASSANDRA_SEEDS="$CASSANDRA_SEEDS<value>$seed2<\/value>"
            fi

            if [ "$seed3" != "$seed2" ] && [ "$seed3" != "$seed1" ] && [ -n "$seed3" ]; then
                CASSANDRA_SEEDS="$CASSANDRA_SEEDS<value>$seed3<\/value>"
            fi

            echo "[INFO] Using Cassandra seeds: $CASSANDRA_SEEDS"

            cat /opt/ignite/config/ignite-cassandra-server-template.xml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS/g" > /opt/ignite/config/ignite-cassandra-server.xml

            return 0
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ $duration -gt $NODE_STARTUP_TIME ]; then
            terminate "${NODE_STARTUP_TIME}min timeout expired, but no Cassandra nodes is up and running"
        fi

        echo "[INFO] Waiting for the first Cassandra node to start and publish its seed, time passed ${duration}min"

        sleep 1m
    done
}

setupIgniteSeeds()
{
    echo "[INFO] Setting up Ignite seeds"

    if [ "$FIRST_NODE" == "true" ]; then
        IGNITE_SEEDS="<value>127.0.0.1:47500..47509<\/value>"
        echo "[INFO] Using localhost address as a seed for the first Ignite node: $IGNITE_SEEDS"
        aws s3 rm --recursive ${S3_IGNITE_NODES_DISCOVERY_URL::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY_URL"
        fi

        cat /opt/ignite/config/ignite-cassandra-server.xml | sed -r "s/\\\$\{IGNITE_SEEDS\}/$IGNITE_SEEDS/g" > /opt/ignite/config/ignite-cassandra-server1.xml
        mv -f /opt/ignite/config/ignite-cassandra-server1.xml /opt/ignite/config/ignite-cassandra-server.xml

        return 0
    fi

    echo "[INFO] Looking for Ignite seeds in: $S3_IGNITE_NODES_DISCOVERY_URL"

    startTime=$(date +%s)

    while true; do
        seeds=$(aws s3 ls $S3_IGNITE_NODES_DISCOVERY_URL | grep -v PRE | sed -r "s/^.* //g")
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

            IGNITE_SEEDS="<value>$seed1<\/value>"

            if [ "$seed2" != "$seed1" ] && [ -n "$seed2" ]; then
                IGNITE_SEEDS="$IGNITE_SEEDS<value>$seed2<\/value>"
            fi

            if [ "$seed3" != "$seed2" ] && [ "$seed3" != "$seed1" ] && [ -n "$seed3" ]; then
                IGNITE_SEEDS="$IGNITE_SEEDS<value>$seed3<\/value>"
            fi

            echo "[INFO] Using Ignite seeds: $IGNITE_SEEDS"

            cat /opt/ignite/config/ignite-cassandra-server.xml | sed -r "s/\\\$\{IGNITE_SEEDS\}/$IGNITE_SEEDS/g" > /opt/ignite/config/ignite-cassandra-server1.xml
            mv -f /opt/ignite/config/ignite-cassandra-server1.xml /opt/ignite/config/ignite-cassandra-server.xml

            return 0
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ $duration -gt $NODE_STARTUP_TIME ]; then
            terminate "${NODE_STARTUP_TIME}min timeout expired, but no Ignite nodes is up and running"
        fi

        echo "[INFO] Waiting for the first Ignite node to start and publish its seed, time passed ${duration}min"

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

    rm -Rf /opt/ignite/first-node-lock

    aws s3 cp $S3_IGNITE_FIRST_NODE_LOCK_URL /opt/ignite/first-node-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created first node lock"
        return 1
    fi

    first_host=$(cat /opt/ignite/first-node-lock)

    rm -f /opt/ignite/first-node-lock

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

    lockExists=$(aws s3 ls $S3_IGNITE_FIRST_NODE_LOCK_URL)
    if [ -n "$lockExists" ]; then
        echo "[INFO] First node lock already exists"
        return 1
    fi

    echo "[INFO] First node lock doesn't exist yet"

    return 0
}

createFirstNodeLock()
{
    aws s3 cp --sse AES256 /opt/ignite/join-lock $S3_IGNITE_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to create first node lock"
    fi
    echo "[INFO] Created first node lock"
}

removeFirstNodeLock()
{
    aws s3 rm $S3_IGNITE_FIRST_NODE_LOCK_URL
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

    rm -Rf /opt/ignite/remote-join-lock

    aws s3 cp $S3_IGNITE_NODES_JOIN_LOCK_URL /opt/ignite/remote-join-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created cluster join lock"
        return 1
    fi

    join_host=$(cat /opt/ignite/remote-join-lock)

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

    lockExists=$(aws s3 ls $S3_IGNITE_NODES_JOIN_LOCK_URL)
    if [ -n "$lockExists" ]; then
        echo "[INFO] Cluster join lock already exists"
        return 1
    fi

    echo "[INFO] Cluster join lock doesn't exist"

    return 0
}

createClusterJoinLock()
{
    aws s3 cp --sse AES256 /opt/ignite/join-lock $S3_IGNITE_NODES_JOIN_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to create cluster join lock"
    fi
    echo "[INFO] Created cluster join lock"
}

removeClusterJoinLock()
{
    aws s3 rm $S3_IGNITE_NODES_JOIN_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to remove cluster join lock"
    fi
    echo "[INFO] Removed cluster join lock"
}

waitToJoinIgniteCluster()
{
    echo "[INFO] Waiting to join Ignite cluster"

    while true; do
        tryToGetClusterJoinLock

        if [ $? -ne 0 ]; then
            echo "[INFO] Another node is trying to join cluster. Waiting for extra 1min."
            sleep 1m
        else
            echo "[INFO]-------------------------------------------------------------"
            echo "[INFO] Congratulations, got lock to join Ignite cluster"
            echo "[INFO]-------------------------------------------------------------"
            break
        fi
    done
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

waitFirstIgniteNodeRegistered()
{
    echo "[INFO] Waiting for the first Ignite node to register"

    startTime=$(date +%s)

    while true; do
        first_host=

        exists=$(aws s3 ls $S3_IGNITE_FIRST_NODE_LOCK_URL)
        if [ -n "$exists" ]; then
            rm -Rf /opt/ignite/first-node-lock

            aws s3 cp $S3_IGNITE_FIRST_NODE_LOCK_URL /opt/ignite/first-node-lock
            if [ $? -ne 0 ]; then
                terminate "Failed to check existing first node lock"
            fi

            first_host=$(cat /opt/ignite/first-node-lock)

            rm -Rf /opt/ignite/first-node-lock
        fi

        if [ -n "$first_host" ]; then
            exists=$(aws s3 ls ${S3_IGNITE_NODES_DISCOVERY_URL}${first_host})
            if [ -n "$exists" ]; then
                break
            fi
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ $duration -gt $NODE_STARTUP_TIME ]; then
            terminate "${NODE_STARTUP_TIME}min timeout expired, but first Ignite node is still not up and running"
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] First Ignite node registered"
}

startIgnite()
{
    echo "[INFO]-------------------------------------------------------------"
    echo "[INFO] Trying attempt $START_ATTEMPT to start Ignite daemon"
    echo "[INFO]-------------------------------------------------------------"
    echo ""

    setupCassandraSeeds
    setupIgniteSeeds

    if [ "$FIRST_NODE" == "true" ]; then
        aws s3 rm --recursive ${S3_IGNITE_NODES_DISCOVERY_URL::-1}
        if [ $? -ne 0 ]; then
            terminate "Failed to clean Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY_URL"
        fi
    else
        waitToJoinIgniteCluster
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

# Time (in minutes) to wait for Ignite/Cassandra daemon up and running
NODE_STARTUP_TIME=10

# Number of attempts to start (not first) Ignite daemon
NODE_START_ATTEMPTS=3

HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
echo $HOST_NAME > /opt/ignite/join-lock

START_ATTEMPT=0

FIRST_NODE="false"

unregisterNode

tryToGetFirstNodeLock

if [ $? -eq 0 ]; then
    FIRST_NODE="true"
fi

echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE" == "true" ]; then
    echo "[INFO] Starting first Ignite node"
else
    echo "[INFO] Starting Ignite node"
fi

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Ignite nodes discovery URL: $S3_IGNITE_NODES_DISCOVERY_URL"
echo "[INFO] Ignite first node lock URL: $S3_IGNITE_FIRST_NODE_LOCK_URL"
echo "[INFO] Cassandra nodes discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
echo "[INFO] Start success URL: $S3_BOOTSTRAP_SUCCESS_URL"
echo "[INFO] Start failure URL: $S3_BOOTSTRAP_FAILURE_URL"
echo "[INFO] IGNITE_HOME: $IGNITE_HOME"
echo "[INFO] JAVA_HOME: $JAVA_HOME"
echo "[INFO] PATH: $PATH"
echo "[INFO]-----------------------------------------------------------------"

if [ -z "$S3_CASSANDRA_NODES_DISCOVERY_URL" ]; then
    terminate "Cassandra S3 discovery URL doesn't specified"
fi

if [[ "$S3_CASSANDRA_NODES_DISCOVERY_URL" != */ ]]; then
    S3_CASSANDRA_NODES_DISCOVERY_URL=${S3_CASSANDRA_NODES_DISCOVERY_URL}/
fi

if [ -z "$S3_IGNITE_NODES_DISCOVERY_URL" ]; then
    terminate "Ignite S3 discovery URL doesn't specified"
fi

if [[ "$S3_IGNITE_NODES_DISCOVERY_URL" != */ ]]; then
    S3_IGNITE_NODES_DISCOVERY_URL=${S3_IGNITE_NODES_DISCOVERY_URL}/
fi

if [ "$FIRST_NODE" != "true" ]; then
    waitFirstIgniteNodeRegistered
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
            terminate "${NODE_STARTUP_TIME}min timeout expired, but first Ignite daemon is still not up and running"
        else
            removeClusterJoinLock

            if [ $START_ATTEMPT -gt $NODE_START_ATTEMPTS ]; then
                terminate "${NODE_START_ATTEMPTS} attempts exceed, but Ignite daemon is still not up and running"
            fi

            startIgnite
        fi

        continue
    fi

    if [ -z "$proc" ]; then
        if [ "$FIRST_NODE" == "true" ]; then
            removeFirstNodeLock
            terminate "Failed to start Ignite daemon"
        fi

        removeClusterJoinLock
        echo "[WARN] Failed to start Ignite daemon. Sleeping for extra 1min"
        sleep 1m
        startIgnite
        continue
    fi

    echo "[INFO] Waiting for Ignite daemon to start, time passed ${duration}min"
    sleep 30s
done

registerNode

terminate