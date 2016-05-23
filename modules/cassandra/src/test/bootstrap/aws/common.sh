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

validate()
{
    if [ -n "$TESTS_TYPE" ] && [ "$TESTS_TYPE" != "ignite" ] && [ "$TESTS_TYPE" != "cassandra" ]; then
        terminate "Incorrect tests type specified: $TESTS_TYPE"
    fi

    if [ -z "$S3_TESTS_NODES_DISCOVERY" ]; then
        terminate "Tests discovery URL doesn't specified"
    fi

    if [[ "$S3_TESTS_NODES_DISCOVERY" != */ ]]; then
        S3_TESTS_NODES_DISCOVERY=${S3_TESTS_NODES_DISCOVERY}/
    fi

    if [ -z "$S3_TESTS_SUCCESS" ]; then
        terminate "Tests success URL doesn't specified"
    fi

    if [[ "$S3_TESTS_SUCCESS" != */ ]]; then
        S3_TESTS_SUCCESS=${S3_TESTS_SUCCESS}/
    fi

    if [ -z "$S3_TESTS_FAILURE" ]; then
        terminate "Tests failure URL doesn't specified"
    fi

    if [[ "$S3_TESTS_FAILURE" != */ ]]; then
        S3_TESTS_FAILURE=${S3_TESTS_FAILURE}/
    fi

    if [ -z "$S3_TESTS_IDLE" ]; then
        terminate "Tests idle URL doesn't specified"
    fi

    if [[ "$S3_TESTS_IDLE" != */ ]]; then
        S3_TESTS_IDLE=${S3_TESTS_IDLE}/
    fi

    if [ -z "$S3_TESTS_PREPARING" ]; then
        terminate "Tests preparing URL doesn't specified"
    fi

    if [[ "$S3_TESTS_PREPARING" != */ ]]; then
        S3_TESTS_PREPARING=${S3_TESTS_PREPARING}/
    fi

    if [ -z "$S3_TESTS_RUNNING" ]; then
        terminate "Tests running URL doesn't specified"
    fi

    if [[ "$S3_TESTS_RUNNING" != */ ]]; then
        S3_TESTS_RUNNING=${S3_TESTS_RUNNING}/
    fi

    if [ -z "$S3_TESTS_WAITING" ]; then
        terminate "Tests waiting URL doesn't specified"
    fi

    if [[ "$S3_TESTS_WAITING" != */ ]]; then
        S3_TESTS_WAITING=${S3_TESTS_WAITING}/
    fi

    if [ -z "$S3_IGNITE_NODES_DISCOVERY" ]; then
        terminate "Ignite discovery URL doesn't specified"
    fi

    if [[ "$S3_IGNITE_NODES_DISCOVERY" != */ ]]; then
        S3_IGNITE_NODES_DISCOVERY=${S3_IGNITE_NODES_DISCOVERY}/
    fi

    if [ -z "$S3_IGNITE_BOOTSTRAP_SUCCESS" ]; then
        terminate "Ignite success URL doesn't specified"
    fi

    if [[ "$S3_IGNITE_BOOTSTRAP_SUCCESS" != */ ]]; then
        S3_IGNITE_BOOTSTRAP_SUCCESS=${S3_IGNITE_BOOTSTRAP_SUCCESS}/
    fi

    if [ -z "$S3_IGNITE_BOOTSTRAP_FAILURE" ]; then
        terminate "Ignite failure URL doesn't specified"
    fi

    if [[ "$S3_IGNITE_BOOTSTRAP_FAILURE" != */ ]]; then
        S3_IGNITE_BOOTSTRAP_FAILURE=${S3_IGNITE_BOOTSTRAP_FAILURE}/
    fi

    if [ -z "$S3_CASSANDRA_NODES_DISCOVERY" ]; then
        terminate "Cassandra discovery URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_NODES_DISCOVERY" != */ ]]; then
        S3_CASSANDRA_NODES_DISCOVERY=${S3_CASSANDRA_NODES_DISCOVERY}/
    fi

    if [ -z "$S3_CASSANDRA_BOOTSTRAP_SUCCESS" ]; then
        terminate "Cassandra success URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_BOOTSTRAP_SUCCESS" != */ ]]; then
        S3_CASSANDRA_BOOTSTRAP_SUCCESS=${S3_CASSANDRA_BOOTSTRAP_SUCCESS}/
    fi

    if [ -z "$S3_CASSANDRA_BOOTSTRAP_FAILURE" ]; then
        terminate "Cassandra failure URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_BOOTSTRAP_FAILURE" != */ ]]; then
        S3_CASSANDRA_BOOTSTRAP_FAILURE=${S3_CASSANDRA_BOOTSTRAP_FAILURE}/
    fi

    if [ -z "$S3_GANGLIA_MASTER_DISCOVERY" ]; then
        terminate "Ganglia master discovery URL doesn't specified"
    fi

    if [[ "$S3_GANGLIA_MASTER_DISCOVERY" != */ ]]; then
        S3_GANGLIA_MASTER_DISCOVERY=${S3_GANGLIA_MASTER_DISCOVERY}/
    fi

    if [ -z "$S3_GANGLIA_BOOTSTRAP_SUCCESS" ]; then
        terminate "Ganglia master success URL doesn't specified"
    fi

    if [[ "$S3_GANGLIA_BOOTSTRAP_SUCCESS" != */ ]]; then
        S3_GANGLIA_BOOTSTRAP_SUCCESS=${S3_GANGLIA_BOOTSTRAP_SUCCESS}/
    fi

    if [ -z "$S3_GANGLIA_BOOTSTRAP_FAILURE" ]; then
        terminate "Ganglia master failure URL doesn't specified"
    fi

    if [[ "$S3_GANGLIA_BOOTSTRAP_FAILURE" != */ ]]; then
        S3_GANGLIA_BOOTSTRAP_FAILURE=${S3_GANGLIA_BOOTSTRAP_FAILURE}/
    fi
}

printInstanceInfo()
{
    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "[INFO] Cassandra download URL: $CASSANDRA_DOWNLOAD_URL"
        echo "[INFO] Tests package download URL: $TESTS_DOWNLOAD_URL"
        echo "[INFO] Ganglia Core download URL: $GANGLIA_CORE_DOWNLOAD_URL"
        echo "[INFO] Ganglia Web download URL: $GANGLIA_WEB_DOWNLOAD_URL"
        echo "[INFO] RRD download URL: $RRD_DOWNLOAD_URL"
        echo "[INFO] Logs URL: $S3_CASSANDRA_LOGS"
        echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER"
        echo "[INFO] Cassandra nodes discovery URL: $S3_CASSANDRA_NODES_DISCOVERY"
        echo "[INFO] Ganglia master discovery URL: $S3_GANGLIA_MASTER_DISCOVERY"
        echo "[INFO] Cassandra first node lock URL: $S3_CASSANDRA_FIRST_NODE_LOCK"
        echo "[INFO] Cassandra nodes join lock URL: $S3_CASSANDRA_NODES_JOIN_LOCK"
        echo "[INFO] Cassandra success URL: $S3_CASSANDRA_BOOTSTRAP_SUCCESS"
        echo "[INFO] Cassandra failure URL: $S3_CASSANDRA_BOOTSTRAP_FAILURE"
    fi

    if [ "$NODE_TYPE" == "ignite" ]; then
        echo "[INFO] Ignite download URL: $IGNITE_DOWNLOAD_URL"
        echo "[INFO] Tests package download URL: $TESTS_DOWNLOAD_URL"
        echo "[INFO] Ganglia Core download URL: $GANGLIA_CORE_DOWNLOAD_URL"
        echo "[INFO] Ganglia Web download URL: $GANGLIA_WEB_DOWNLOAD_URL"
        echo "[INFO] RRD download URL: $RRD_DOWNLOAD_URL"
        echo "[INFO] Logs URL: $S3_IGNITE_LOGS"
        echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER"
        echo "[INFO] Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY"
        echo "[INFO] Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY"
        echo "[INFO] Ganglia master discovery URL: $S3_GANGLIA_MASTER_DISCOVERY"
        echo "[INFO] Ignite first node lock URL: $S3_IGNITE_FIRST_NODE_LOCK"
        echo "[INFO] Ignite nodes join lock URL: $S3_IGNITE_NODES_JOIN_LOCK"
        echo "[INFO] Ignite success URL: $S3_IGNITE_BOOTSTRAP_SUCCESS"
        echo "[INFO] Ignite failure URL: $S3_IGNITE_BOOTSTRAP_FAILURE"
    fi

    if [ "$NODE_TYPE" == "test" ]; then
        echo "[INFO] Tests type: $TESTS_TYPE"
        echo "[INFO] Test nodes count: $TEST_NODES_COUNT"
        echo "[INFO] Ignite nodes count: $IGNITE_NODES_COUNT"
        echo "[INFO] Cassandra nodes count: $CASSANDRA_NODES_COUNT"
        echo "[INFO] Tests summary URL: $S3_TESTS_SUMMARY"
        echo "[INFO] ----------------------------------------------------"
        echo "[INFO] Tests package download URL: $TESTS_DOWNLOAD_URL"
        echo "[INFO] Ganglia Core download URL: $GANGLIA_CORE_DOWNLOAD_URL"
        echo "[INFO] Ganglia Web download URL: $GANGLIA_WEB_DOWNLOAD_URL"
        echo "[INFO] RRD download URL: $RRD_DOWNLOAD_URL"
        echo "[INFO] Logs URL: $S3_TESTS_LOGS"
        echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER"
        echo "[INFO] Test node discovery URL: $S3_TESTS_NODES_DISCOVERY"
        echo "[INFO] Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY"
        echo "[INFO] Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY"
        echo "[INFO] Ganglia master discovery URL: $S3_GANGLIA_MASTER_DISCOVERY"
        echo "[INFO] Tests trigger URL: $S3_TESTS_TRIGGER"
        echo "[INFO] Tests idle URL: $S3_TESTS_IDLE"
        echo "[INFO] Tests preparing URL: $S3_TESTS_PREPARING"
        echo "[INFO] Tests waiting URL: $S3_TESTS_WAITING"
        echo "[INFO] Tests running URL: $S3_TESTS_RUNNING"
        echo "[INFO] Tests success URL: $S3_TESTS_SUCCESS"
        echo "[INFO] Tests failure URL: $S3_TESTS_FAILURE"
        echo "[INFO] Ignite success URL: $S3_IGNITE_BOOTSTRAP_SUCCESS"
        echo "[INFO] Ignite failure URL: $S3_IGNITE_BOOTSTRAP_FAILURE"
        echo "[INFO] Cassandra success URL: $S3_CASSANDRA_BOOTSTRAP_SUCCESS"
        echo "[INFO] Cassandra failure URL: $S3_CASSANDRA_BOOTSTRAP_FAILURE"
    fi

    if [ "$NODE_TYPE" == "ganglia" ]; then
        echo "[INFO] Ganglia Core download URL: $GANGLIA_CORE_DOWNLOAD_URL"
        echo "[INFO] Ganglia Web download URL: $GANGLIA_WEB_DOWNLOAD_URL"
        echo "[INFO] RRD download URL: $RRD_DOWNLOAD_URL"
        echo "[INFO] Tests package download URL: $TESTS_DOWNLOAD_URL"
        echo "[INFO] Logs URL: $S3_GANGLIA_LOGS"
        echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER"
        echo "[INFO] Ganglia master discovery URL: $S3_GANGLIA_MASTER_DISCOVERY"
        echo "[INFO] Ganglia success URL: $S3_GANGLIA_BOOTSTRAP_SUCCESS"
        echo "[INFO] Ganglia failure URL: $S3_GANGLIA_BOOTSTRAP_FAILURE"
    fi
}

tagInstance()
{
    export EC2_HOME=/opt/aws/apitools/ec2
    export JAVA_HOME=/opt/java
    export PATH=$JAVA_HOME/bin:$EC2_HOME/bin:$PATH

    INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to get instance metadata to tag it"
        exit 1
    fi

    INSTANCE_NAME=

    if [ "$NODE_TYPE" == "cassandra" ]; then
        INSTANCE_NAME=$EC2_CASSANDRA_TAG
    elif [ "$NODE_TYPE" == "ignite" ]; then
        INSTANCE_NAME=$EC2_IGNITE_TAG
    elif [ "$NODE_TYPE" == "test" ]; then
        INSTANCE_NAME=$EC2_TEST_TAG
    elif [ "$NODE_TYPE" == "ganglia" ]; then
        INSTANCE_NAME=$EC2_GANGLIA_TAG
    fi

    if [ -n "$INSTANCE_NAME" ]; then
        ec2-create-tags $INSTANCE_ID --tag Name=${INSTANCE_NAME} --region $EC2_INSTANCE_REGION
        if [ $code -ne 0 ]; then
            echo "[ERROR] Failed to tag EC2 instance with: Name=${INSTANCE_NAME}"
            exit 1
        fi
    fi

    if [ -n "$EC2_OWNER_TAG" ]; then
        ec2-create-tags $INSTANCE_ID --tag owner=${EC2_OWNER_TAG} --region $EC2_INSTANCE_REGION
        if [ $code -ne 0 ]; then
            echo "[ERROR] Failed to tag EC2 instance with: owner=${EC2_OWNER_TAG}"
            exit 1
        fi
    fi

    if [ -n "$EC2_PROJECT_TAG" ]; then
        ec2-create-tags $INSTANCE_ID --tag project=${EC2_PROJECT_TAG} --region $EC2_INSTANCE_REGION
        if [ $code -ne 0 ]; then
            echo "[ERROR] Failed to tag EC2 instance with: project=${EC2_PROJECT_TAG}"
            exit 1
        fi
    fi
}

setNodeType()
{
    if [ -n "$1" ]; then
        NEW_NODE_TYPE=$NODE_TYPE
        NODE_TYPE=$1
    else
        NEW_NODE_TYPE=
    fi
}

revertNodeType()
{
    if [ -n "$NEW_NODE_TYPE" ]; then
        NODE_TYPE=$NEW_NODE_TYPE
        NEW_NODE_TYPE=
    fi
}

getLocalLogsFolder()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "/opt/cassandra/logs"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "/opt/ignite/work/log"
    elif [ "$NODE_TYPE" == "test" ]; then
        echo "/opt/ignite-cassandra-tests/logs"
    elif [ "$NODE_TYPE" == "ganglia" ]; then
        echo ""
    fi

    revertNodeType
}

getDiscoveryUrl()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "$S3_CASSANDRA_NODES_DISCOVERY"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "$S3_IGNITE_NODES_DISCOVERY"
    elif [ "$NODE_TYPE" == "test" ]; then
        echo "$S3_TESTS_NODES_DISCOVERY"
    elif [ "$NODE_TYPE" == "ganglia" ]; then
        echo "$S3_GANGLIA_MASTER_DISCOVERY"
    fi

    revertNodeType
}

getJoinLockUrl()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "$S3_CASSANDRA_NODES_JOIN_LOCK"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "$S3_IGNITE_NODES_JOIN_LOCK"
    fi

    revertNodeType
}

getFirstNodeLockUrl()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "$S3_CASSANDRA_FIRST_NODE_LOCK"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "$S3_IGNITE_FIRST_NODE_LOCK"
    elif [ "$NODE_TYPE" == "test" ]; then
        echo "$S3_TESTS_FIRST_NODE_LOCK"
    fi

    revertNodeType
}

getSucessUrl()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "$S3_CASSANDRA_BOOTSTRAP_SUCCESS"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "$S3_IGNITE_BOOTSTRAP_SUCCESS"
    elif [ "$NODE_TYPE" == "test" ]; then
        echo "$S3_TESTS_SUCCESS"
    elif [ "$NODE_TYPE" == "ganglia" ]; then
        echo "$S3_GANGLIA_BOOTSTRAP_SUCCESS"
    fi

    revertNodeType
}

getFailureUrl()
{
    setNodeType $1

    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "$S3_CASSANDRA_BOOTSTRAP_FAILURE"
    elif [ "$NODE_TYPE" == "ignite" ]; then
        echo "$S3_IGNITE_BOOTSTRAP_FAILURE"
    elif [ "$NODE_TYPE" == "test" ]; then
        echo "$S3_TESTS_FAILURE"
    elif [ "$NODE_TYPE" == "ganglia" ]; then
        echo "$S3_GANGLIA_BOOTSTRAP_FAILURE"
    fi

    revertNodeType
}

terminate()
{
    SUCCESS_URL=$(getSucessUrl)
    FAILURE_URL=$(getFailureUrl)

    if [ -n "$SUCCESS_URL" ] && [[ "$SUCCESS_URL" != */ ]]; then
        SUCCESS_URL=${SUCCESS_URL}/
    fi

    if [ -n "$FAILURE_URL" ] && [[ "$FAILURE_URL" != */ ]]; then
        FAILURE_URL=${FAILURE_URL}/
    fi

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    msg=$HOST_NAME

    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Failed to start $NODE_TYPE node"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1
        reportFolder=${FAILURE_URL}${HOST_NAME}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] $NODE_TYPE node successfully started"
        echo "[INFO]-----------------------------------------------------"
        reportFolder=${SUCCESS_URL}${HOST_NAME}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/ignite-cassandra-tests/bootstrap/start_result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to drop report folder: $reportFolder"
    fi

    localLogs=$(getLocalLogsFolder)

    if [ -d "$localLogs" ]; then
        aws s3 sync --sse AES256 $localLogs $reportFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export $NODE_TYPE logs to: $reportFolder"
        fi
    fi

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/bootstrap/start_result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to export node start result to: $reportFile"
    fi

    rm -f /opt/ignite-cassandra-tests/bootstrap/start_result /opt/ignite-cassandra-tests/bootstrap/join-lock /opt/ignite-cassandra-tests/bootstrap/first-node-lock

    removeFirstNodeLock
    removeClusterJoinLock

    if [ "$NODE_TYPE" == "test" ]; then
        aws s3 rm ${S3_TESTS_RUNNING}${HOST_NAME}
        aws s3 rm ${S3_TESTS_WAITING}${HOST_NAME}
        aws s3 rm ${S3_TESTS_IDLE}${HOST_NAME}
        aws s3 rm ${S3_TESTS_PREPARING}${HOST_NAME}
        unregisterNode
    fi

    if [ -n "$1" ]; then
        unregisterNode
        exit 1
    fi

    exit 0
}

registerNode()
{
    DISCOVERY_URL=$(getDiscoveryUrl)
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Registering $NODE_TYPE node: ${DISCOVERY_URL}${HOST_NAME}"

    aws s3 cp --sse AES256 /etc/hosts ${DISCOVERY_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to register $NODE_TYPE node info in: ${DISCOVERY_URL}${HOST_NAME}"
    fi

    echo "[INFO] $NODE_TYPE node successfully registered"
}

unregisterNode()
{
    DISCOVERY_URL=$(getDiscoveryUrl)
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Removing $NODE_TYPE node registration from: ${DISCOVERY_URL}${HOST_NAME}"

    aws s3 rm ${DISCOVERY_URL}${HOST_NAME}

    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to remove $NODE_TYPE node registration"
    else
        echo "[INFO] $NODE_TYPE node registration removed"
    fi
}

cleanupMetadata()
{
    DISCOVERY_URL=$(getDiscoveryUrl)
    JOIN_LOCK_URL=$(getJoinLockUrl)
    SUCCESS_URL=$(getSucessUrl)
    FAILURE_URL=$(getFailureUrl)

    echo "[INFO] Running cleanup"

    aws s3 rm $JOIN_LOCK_URL
    aws s3 rm --recursive $DISCOVERY_URL
    aws s3 rm --recursive $SUCCESS_URL
    aws s3 rm --recursive $FAILURE_URL

    echo "[INFO] Cleanup completed"
}

tryToGetFirstNodeLock()
{
    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        return 0
    fi

    FIRST_NODE_LOCK_URL=$(getFirstNodeLockUrl)
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Trying to get first node lock: $FIRST_NODE_LOCK_URL"

    checkFirstNodeLockExist $FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        return 1
    fi

    echo "$HOST_NAME" > /opt/ignite-cassandra-tests/bootstrap/first-node-lock

    createFirstNodeLock $FIRST_NODE_LOCK_URL

    sleep 5s

    rm -Rf /opt/ignite-cassandra-tests/bootstrap/first-node-lock

    aws s3 cp $FIRST_NODE_LOCK_URL /opt/ignite-cassandra-tests/bootstrap/first-node-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created first node lock"
        return 1
    fi

    first_host=$(cat /opt/ignite-cassandra-tests/bootstrap/first-node-lock)

    rm -f /opt/ignite-cassandra-tests/bootstrap/first-node-lock

    if [ "$first_host" != "$HOST_NAME" ]; then
        echo "[INFO] Node $first_host has discarded previously created first node lock"
        return 1
    fi

    echo "[INFO] Congratulations, got first node lock"

    FIRST_NODE_LOCK="true"

    return 0
}

checkFirstNodeLockExist()
{
    echo "[INFO] Checking for the first node lock: $1"

    lockExists=$(aws s3 ls $1)
    if [ -n "$lockExists" ]; then
        echo "[INFO] First node lock already exists"
        return 1
    fi

    echo "[INFO] First node lock doesn't exist"

    return 0
}

createFirstNodeLock()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/bootstrap/first-node-lock $1

    if [ $? -ne 0 ]; then
        terminate "Failed to create first node lock: $1"
    fi

    echo "[INFO] Created first node lock: $1"
}

removeFirstNodeLock()
{
    if [ "$FIRST_NODE_LOCK" != "true" ]; then
        return 0
    fi

    FIRST_NODE_LOCK_URL=$(getFirstNodeLockUrl)

    echo "[INFO] Removing first node lock: $FIRST_NODE_LOCK_URL"

    aws s3 rm $FIRST_NODE_LOCK_URL

    if [ $? -ne 0 ]; then
        terminate "Failed to remove first node lock: $FIRST_NODE_LOCK_URL"
    fi

    echo "[INFO] Removed first node lock: $FIRST_NODE_LOCK_URL"

    FIRST_NODE_LOCK="false"
}

tryToGetClusterJoinLock()
{
    if [ "$JOIN_LOCK" == "true" ]; then
        return 0
    fi

    JOIN_LOCK_URL=$(getJoinLockUrl)
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Trying to get cluster join lock"

    checkClusterJoinLockExist $JOIN_LOCK_URL
    if [ $? -ne 0 ]; then
        return 1
    fi

    echo "$HOST_NAME" > /opt/ignite-cassandra-tests/bootstrap/join-lock

    createClusterJoinLock $JOIN_LOCK_URL

    sleep 5s

    rm -Rf /opt/ignite-cassandra-tests/bootstrap/join-lock

    aws s3 cp $JOIN_LOCK_URL /opt/ignite-cassandra-tests/bootstrap/join-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created cluster join lock"
        return 1
    fi

    join_host=$(cat /opt/ignite-cassandra-tests/bootstrap/join-lock)

    if [ "$join_host" != "$HOST_NAME" ]; then
        echo "[INFO] Node $first_host has discarded previously created cluster join lock"
        return 1
    fi

    echo "[INFO] Congratulations, got cluster join lock"

    JOIN_LOCK="true"

    return 0
}

checkClusterJoinLockExist()
{
    echo "[INFO] Checking for the cluster join lock: $1"

    lockExists=$(aws s3 ls $1)
    if [ -n "$lockExists" ]; then
        echo "[INFO] Cluster join lock already exists"
        return 1
    fi

    if [ "$NODE_TYPE" == "cassandra" ]; then
        status=$(/opt/cassandra/bin/nodetool -h $CASSANDRA_SEED status)
        leaving=$(echo $status | grep UL)
        moving=$(echo $status | grep UM)
        joining=$(echo $status | grep UJ)

        if [ -n "$leaving" ] || [ -n "$moving" ] || [ -n "$joining" ]; then
            echo "[INFO] Cluster join lock doesn't exist in S3, but some node still trying to join Cassandra cluster"
            return 1
        fi
    fi

    echo "[INFO] Cluster join lock doesn't exist"
}

createClusterJoinLock()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/bootstrap/join-lock $1

    if [ $? -ne 0 ]; then
        terminate "Failed to create cluster join lock: $1"
    fi

    echo "[INFO] Created cluster join lock: $1"
}

removeClusterJoinLock()
{
    if [ "$JOIN_LOCK" != "true" ]; then
        return 0
    fi

    JOIN_LOCK_URL=$(getJoinLockUrl)

    echo "[INFO] Removing cluster join lock: $JOIN_LOCK_URL"

    aws s3 rm $JOIN_LOCK_URL

    if [ $? -ne 0 ]; then
        terminate "Failed to remove cluster join lock: $JOIN_LOCK_URL"
    fi

    JOIN_LOCK="false"

    echo "[INFO] Removed cluster join lock: $JOIN_LOCK_URL"
}

waitToJoinCluster()
{
    echo "[INFO] Waiting to join $NODE_TYPE cluster"

    while true; do
        tryToGetClusterJoinLock

        if [ $? -ne 0 ]; then
            echo "[INFO] Another node is trying to join cluster. Waiting for extra 30sec."
            sleep 30s
        else
            echo "[INFO]-------------------------------------------------------------"
            echo "[INFO] Congratulations, got lock to join $NODE_TYPE cluster"
            echo "[INFO]-------------------------------------------------------------"
            break
        fi
    done
}

setupClusterSeeds()
{
    if [ "$1" != "cassandra" ] && [ "$1" != "ignite" ]; then
        terminate "Incorrect cluster type specified '$1' to setup seeds"
    fi

    DISCOVERY_URL=$(getDiscoveryUrl $1)

    echo "[INFO] Setting up $1 seeds"

    echo "[INFO] Looking for $1 seeds in: $DISCOVERY_URL"

    startTime=$(date +%s)

    while true; do
        seeds=$(aws s3 ls $DISCOVERY_URL | grep -v PRE | sed -r "s/^.* //g")
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

            CLUSTER_SEEDS=$seed1

            if [ "$seed2" != "$seed1" ] && [ -n "$seed2" ]; then
                CLUSTER_SEEDS="$CLUSTER_SEEDS $seed2"
            fi

            if [ "$seed3" != "$seed2" ] && [ "$seed3" != "$seed1" ] && [ -n "$seed3" ]; then
                CLUSTER_SEEDS="$CLUSTER_SEEDS $seed3"
            fi

            echo "[INFO] Using $1 seeds: $CLUSTER_SEEDS"

            return 0
        fi

        if [ "$2" == "true" ]; then
            currentTime=$(date +%s)
            duration=$(( $currentTime-$startTime ))
            duration=$(( $duration/60 ))

            if [ $duration -gt $SERVICE_STARTUP_TIME ]; then
                terminate "${SERVICE_STARTUP_TIME}min timeout expired, but first $1 node is still not up and running"
            fi
        fi

        echo "[INFO] Waiting for the first $1 node to start and publish its seed, time passed ${duration}min"

        sleep 30s
    done
}

waitFirstClusterNodeRegistered()
{
    DISCOVERY_URL=$(getDiscoveryUrl)

    echo "[INFO] Waiting for the first $NODE_TYPE node to register"

    startTime=$(date +%s)

    while true; do
        exists=$(aws s3 ls $DISCOVERY_URL)
        if [ -n "$exists" ]; then
            break
        fi

        if [ "$1" == "true" ]; then
            currentTime=$(date +%s)
            duration=$(( $currentTime-$startTime ))
            duration=$(( $duration/60 ))

            if [ $duration -gt $SERVICE_STARTUP_TIME ]; then
                terminate "${SERVICE_STARTUP_TIME}min timeout expired, but first $type node is still not up and running"
            fi
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    echo "[INFO] First $type node registered"
}

waitAllClusterNodesReady()
{
    if [ "$1" == "cassandra" ]; then
        NODES_COUNT=$CASSANDRA_NODES_COUNT
    elif [ "$1" == "ignite" ]; then
        NODES_COUNT=$IGNITE_NODES_COUNT
    elif [ "$1" == "test" ]; then
        NODES_COUNT=$TEST_NODES_COUNT
    else
        terminate "Incorrect cluster type specified '$1' to wait for all nodes up and running"
    fi

    SUCCESS_URL=$(getSucessUrl $1)

    if [ $NODES_COUNT -eq 0 ]; then
        return 0
    fi

    echo "[INFO] Waiting for all $NODES_COUNT $1 nodes ready"

    while true; do
        if [ "$1" == "test" ]; then
            count1=$(aws s3 ls $S3_TESTS_WAITING | wc -l)
            count2=$(aws s3 ls $S3_TESTS_RUNNING | wc -l)
            count=$(( $count1+$count2 ))
        else
            count=$(aws s3 ls $SUCCESS_URL | wc -l)
        fi

        if [ $count -ge $NODES_COUNT ]; then
            break
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    sleep 30s

    echo "[INFO] Congratulation, all $NODES_COUNT $1 nodes are ready"
}

waitAllTestNodesCompletedTests()
{
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Waiting for all $TEST_NODES_COUNT test nodes to complete their tests"

    while true; do

        count=$(aws s3 ls $S3_TESTS_RUNNING | grep -v $HOST_NAME | wc -l)

        if [ $count -eq 0 ]; then
            break
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    echo "[INFO] Congratulation, all $TEST_NODES_COUNT test nodes have completed their tests"
}

. $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/env.sh

validate

if [ "$1" != "cassandra" ] && [ "$1" != "ignite" ] && [ "$1" != "test" ] && [ "$1" != "ganglia" ]; then
    echo "[ERROR] Unsupported node type specified: $1"
    exit 1
fi

export NODE_TYPE=$1
