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

# -----------------------------------------------------------------------------------------------
# Common purpose functions used by bootstrap scripts
# -----------------------------------------------------------------------------------------------

# Validates values of the main environment variables specified in env.sh
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

# Prints EC2 instance info
printInstanceInfo()
{
    if [ "$NODE_TYPE" == "cassandra" ]; then
        echo "[INFO] Cassandra download URL: $CASSANDRA_DOWNLOAD_URL"
        echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
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
        echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
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
        echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
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
        echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
        echo "[INFO] Logs URL: $S3_GANGLIA_LOGS"
        echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER"
        echo "[INFO] Ganglia master discovery URL: $S3_GANGLIA_MASTER_DISCOVERY"
        echo "[INFO] Ganglia success URL: $S3_GANGLIA_BOOTSTRAP_SUCCESS"
        echo "[INFO] Ganglia failure URL: $S3_GANGLIA_BOOTSTRAP_FAILURE"
    fi
}

# Clone git repository
gitClone()
{
    echo "[INFO] Cloning git repository $1 to $2"

    rm -Rf $2

    for i in 0 9;
    do
        git clone $1 $2

        if [ $code -eq 0 ]; then
            echo "[INFO] Git repository $1 was successfully cloned to $2"
            return 0
        fi

        echo "[WARN] Failed to clone git repository $1 from $i attempt, sleeping extra 5sec"
        rm -Rf $2
        sleep 5s
    done

    terminate "All 10 attempts to clone git repository $1 are failed"
}

# Applies specified tag to EC2 instance
createTag()
{
    if [ -z "$EC2_INSTANCE_REGION" ]; then
        EC2_AVAIL_ZONE=`curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone`
        EC2_INSTANCE_REGION="`echo \"$EC2_AVAIL_ZONE\" | sed -e 's:\([0-9][0-9]*\)[a-z]*\$:\\1:'`"
        export EC2_INSTANCE_REGION
        echo "[INFO] EC2 instance region: $EC2_INSTANCE_REGION"
    fi

    for i in 0 9;
    do
        aws ec2 create-tags --resources $1 --tags Key=$2,Value=$3 --region $EC2_INSTANCE_REGION
        if [ $? -eq 0 ]; then
            return 0
        fi

        echo "[WARN] $i attempt to tag EC2 instance $1 with $2=$3 is failed, sleeping extra 5sec"
        sleep 5s
    done

    terminate "All 10 attempts to tag EC2 instance $1 with $2=$3 are failed"
}

# Applies 'owner', 'project' and 'Name' tags to EC2 instance
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
        createTag "$INSTANCE_ID" "Name" "${INSTANCE_NAME}"
    fi

    if [ -n "$EC2_OWNER_TAG" ]; then
        createTag "$INSTANCE_ID" "owner" "${EC2_OWNER_TAG}"
    fi

    if [ -n "$EC2_PROJECT_TAG" ]; then
        createTag "$INSTANCE_ID" "project" "${EC2_PROJECT_TAG}"
    fi
}

# Sets NODE_TYPE env variable
setNodeType()
{
    if [ -n "$1" ]; then
        NEW_NODE_TYPE=$NODE_TYPE
        NODE_TYPE=$1
    else
        NEW_NODE_TYPE=
    fi
}

# Reverts NODE_TYPE env variable to previous value
revertNodeType()
{
    if [ -n "$NEW_NODE_TYPE" ]; then
        NODE_TYPE=$NEW_NODE_TYPE
        NEW_NODE_TYPE=
    fi
}

# Returns logs folder for the node (Cassandra, Ignite, Tests)
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

# Returns S3 URL to discover this node
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

# Returns S3 URL used as a join lock, used by nodes to join cluster sequentially
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

# Returns S3 URL used to select first node for the cluster. The first node is responsible
# for doing all routine work (clean S3 logs/test results from previous execution) on cluster startup
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

# Returns S3 success URL for the node - folder created in S3 in case node successfully started and containing node logs
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

# Returns S3 failure URL for the node - folder created in S3 in case node failed to start and containing node logs
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

# Terminates script execution, unregisters node and removes all the locks (join lock, first node lock) created by it
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

    removeClusterJoinLock

    if [ "$NODE_TYPE" == "test" ]; then
        aws s3 rm ${S3_TESTS_RUNNING}${HOST_NAME}
        aws s3 rm ${S3_TESTS_WAITING}${HOST_NAME}
        aws s3 rm ${S3_TESTS_IDLE}${HOST_NAME}
        aws s3 rm ${S3_TESTS_PREPARING}${HOST_NAME}
        unregisterNode
    fi

    if [ -n "$1" ]; then
        removeFirstNodeLock
        unregisterNode
        exit 1
    fi

    exit 0
}

# Registers node by creating a file having node hostname inside specific folder in S3
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

# Unregisters node by removing a file having node hostname inside specific folder in S3
unregisterNode()
{
    DISCOVERY_URL=$(getDiscoveryUrl)
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "[INFO] Removing $NODE_TYPE node registration from: ${DISCOVERY_URL}${HOST_NAME}"

    exists=$(aws s3 ls ${DISCOVERY_URL}${HOST_NAME})

    if [ -n "$exists" ]; then
        aws s3 rm ${DISCOVERY_URL}${HOST_NAME}

        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to remove $NODE_TYPE node registration"
        else
            echo "[INFO] $NODE_TYPE node registration removed"
        fi
    else
        echo "[INFO] Node registration actually haven't been previously created"
    fi
}

# Cleans up all nodes metadata for particular cluster (Cassandra, Ignite, Tests). Performed only by the node acquired
# first node lock.
cleanupMetadata()
{
    DISCOVERY_URL=$(getDiscoveryUrl)
    JOIN_LOCK_URL=$(getJoinLockUrl)
    SUCCESS_URL=$(getSucessUrl)
    FAILURE_URL=$(getFailureUrl)

    echo "[INFO] Running metadata cleanup"

    aws s3 rm $JOIN_LOCK_URL
    aws s3 rm --recursive $DISCOVERY_URL
    aws s3 rm --recursive $SUCCESS_URL
    aws s3 rm --recursive $FAILURE_URL

    echo "[INFO] Metadata cleanup completed"
}

# Tries to get first node lock for the node. Only one (first) node can have such lock and it will be responsible for
# cleanup process when starting cluster
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

# Checks if first node lock already exists in S3
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

# Creates first node lock in S3
createFirstNodeLock()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/bootstrap/first-node-lock $1

    if [ $? -ne 0 ]; then
        terminate "Failed to create first node lock: $1"
    fi

    echo "[INFO] Created first node lock: $1"
}

# Removes first node lock from S3
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

# Tries to get cluster join lock. Nodes use this lock to join a cluster sequentially.
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

# Checks if join lock already exists in S3
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

# Creates join lock in S3
createClusterJoinLock()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/bootstrap/join-lock $1

    if [ $? -ne 0 ]; then
        terminate "Failed to create cluster join lock: $1"
    fi

    echo "[INFO] Created cluster join lock: $1"
}

# Removes join lock
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

# Waits for the node to join cluster, periodically trying to acquire cluster join lock and exiting only when node
# successfully acquired the lock. Such mechanism used by nodes to join cluster sequentially (limitation of Cassandra).
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

# Wait for the cluster to register at least one node in S3, so that all other nodes will use already existing nodes
# to send them info about them and join the cluster
setupClusterSeeds()
{
    if [ "$1" != "cassandra" ] && [ "$1" != "ignite" ] && [ "$1" != "test" ]; then
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

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        if [ "$2" == "true" ]; then
            if [ $duration -gt $SERVICE_STARTUP_TIME ]; then
                terminate "${SERVICE_STARTUP_TIME}min timeout expired, but first $1 node is still not up and running"
            fi
        fi

        echo "[INFO] Waiting for the first $1 node to start and publish its seed, time passed ${duration}min"

        sleep 30s
    done
}

# Wait until first cluster node registered in S3
waitFirstClusterNodeRegistered()
{
    DISCOVERY_URL=$(getDiscoveryUrl)

    echo "[INFO] Waiting for the first $NODE_TYPE node to register in: $DISCOVERY_URL"

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

# Waits until all cluster nodes successfully bootstrapped. In case of Tests cluster also waits until all nodes
# switch to waiting state
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

# Wait untill all Tests cluster nodes completed their tests execution
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

# Installs all required Ganglia packages
installGangliaPackages()
{
    if [ "$1" == "master" ]; then
        echo "[INFO] Installing Ganglia master required packages"
    else
        echo "[INFO] Installing Ganglia agent required packages"
    fi

    isAmazonLinux=$(cat "/etc/issue" | grep "Amazon Linux")

    if [ -z "$isAmazonLinux" ]; then
        setenforce 0

        if [ $? -ne 0 ]; then
            terminate "Failed to turn off SELinux"
        fi

        downloadPackage "$EPEL_DOWNLOAD_URL" "/opt/epel.rpm" "EPEL"

        rpm -Uvh /opt/epel.rpm
        if [ $? -ne 0 ]; then
            terminate "Failed to setup EPEL repository"
        fi

        rm -f /opt/epel.rpm
    fi

    yum -y install apr-devel apr-util check-devel cairo-devel pango-devel pango \
    libxml2-devel glib2-devel dbus-devel freetype-devel freetype \
    libpng-devel libart_lgpl-devel fontconfig-devel gcc-c++ expat-devel \
    python-devel libXrender-devel perl-devel perl-CPAN gettext git sysstat \
    automake autoconf ltmain.sh pkg-config gperf libtool pcre-devel libconfuse-devel

    if [ $? -ne 0 ]; then
        terminate "Failed to install all Ganglia required packages"
    fi

    if [ "$1" == "master" ]; then
        yum -y install httpd php php-devel php-pear

        if [ $? -ne 0 ]; then
            terminate "Failed to install all Ganglia required packages"
        fi

        if [ -z "$isAmazonLinux" ]; then
            yum -y install liberation-sans-fonts

            if [ $? -ne 0 ]; then
                terminate "Failed to install liberation-sans-fonts package"
            fi
        fi
    fi

    if [ -z "$isAmazonLinux" ]; then
        downloadPackage "$GPERF_DOWNLOAD_URL" "/opt/gperf.tar.gz" "gperf"

        tar -xvzf /opt/gperf.tar.gz -C /opt
        if [ $? -ne 0 ]; then
            terminate "Failed to untar gperf tarball"
        fi

        rm -Rf /opt/gperf.tar.gz

        unzipDir=$(ls /opt | grep "gperf")

        if [ $? -ne 0 ]; then
            terminate "Failed to update creation date to current for all files inside: /opt/$unzipDir"
        fi

        pushd /opt/$unzipDir

        cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
        rm ./configure
        mv ./configure1 ./configure
        chmod a+x ./configure

        ./configure
        if [ $? -ne 0 ]; then
            terminate "Failed to configure gperf"
        fi

        make
        if [ $? -ne 0 ]; then
            terminate "Failed to make gperf"
        fi

        make install
        if [ $? -ne 0 ]; then
            terminate "Failed to install gperf"
        fi

        echo "[INFO] gperf tool successfully installed"

        popd
    fi

    echo "[INFO] Installing rrdtool"

    downloadPackage "$RRD_DOWNLOAD_URL" "/opt/rrdtool.tar.gz" "rrdtool"

    tar -xvzf /opt/rrdtool.tar.gz -C /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to untar rrdtool tarball"
    fi

    rm -Rf /opt/rrdtool.tar.gz

    unzipDir=$(ls /opt | grep "rrdtool")
    if [ "$unzipDir" != "rrdtool" ]; then
        mv /opt/$unzipDir /opt/rrdtool
    fi

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/rrdtool"
    fi

    export PKG_CONFIG_PATH=/usr/lib/pkgconfig/

    pushd /opt/rrdtool

    cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
    rm ./configure
    mv ./configure1 ./configure
    chmod a+x ./configure

    ./configure --prefix=/usr/local/rrdtool
    if [ $? -ne 0 ]; then
        terminate "Failed to configure rrdtool"
    fi

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make rrdtool"
    fi

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install rrdtool"
    fi

    ln -s /usr/local/rrdtool/bin/rrdtool /usr/bin/rrdtool
    mkdir -p /var/lib/ganglia/rrds

    chown -R nobody:nobody /usr/local/rrdtool /var/lib/ganglia/rrds /usr/bin/rrdtool

    rm -Rf /opt/rrdtool

    popd

    echo "[INFO] rrdtool successfully installed"

    echo "[INFO] Installig ganglia-core"

    gitClone $GANGLIA_CORE_DOWNLOAD_URL /opt/monitor-core

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/monitor-core"
    fi

    pushd /opt/monitor-core

    git checkout efe9b5e5712ea74c04e3b15a06eb21900e18db40

    ./bootstrap

    if [ $? -ne 0 ]; then
        terminate "Failed to prepare ganglia-core for compilation"
    fi

    cat ./configure | sed -r "s/test \"\\\$2\" = conftest.file/test 1 = 1/g" > ./configure1
    rm ./configure
    mv ./configure1 ./configure
    chmod a+x ./configure

    ./configure --with-gmetad --with-librrd=/usr/local/rrdtool

    if [ $? -ne 0 ]; then
        terminate "Failed to configure ganglia-core"
    fi

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make ganglia-core"
    fi

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install ganglia-core"
    fi

    rm -Rf /opt/monitor-core

    popd

    echo "[INFO] ganglia-core successfully installed"

    if [ "$1" != "master" ]; then
        return 0
    fi

    echo "[INFO] Installing ganglia-web"

    gitClone $GANGLIA_WEB_DOWNLOAD_URL /opt/web

    if [ $? -ne 0 ]; then
        terminate "Failed to update creation date to current for all files inside: /opt/web"
    fi

    cat /opt/web/Makefile | sed -r "s/GDESTDIR = \/usr\/share\/ganglia-webfrontend/GDESTDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile1
    cat /opt/web/Makefile1 | sed -r "s/GCONFDIR = \/etc\/ganglia-web/GCONFDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile2
    cat /opt/web/Makefile2 | sed -r "s/GWEB_STATEDIR = \/var\/lib\/ganglia-web/GWEB_STATEDIR = \/opt\/ganglia-web/g" > /opt/web/Makefile3
    cat /opt/web/Makefile3 | sed -r "s/APACHE_USER = www-data/APACHE_USER = apache/g" > /opt/web/Makefile4

    rm -f /opt/web/Makefile
    cp /opt/web/Makefile4 /opt/web/Makefile
    rm -f /opt/web/Makefile1 /opt/web/Makefile2 /opt/web/Makefile3 /opt/web/Makefile4

    pushd /opt/web

    git checkout f2b19c7cacfc8c51921be801b92f8ed0bd4901ae

    make

    if [ $? -ne 0 ]; then
        terminate "Failed to make ganglia-web"
    fi

    make install

    if [ $? -ne 0 ]; then
        terminate "Failed to install ganglia-web"
    fi

    rm -Rf /opt/web

    popd

    echo "" >> /etc/httpd/conf/httpd.conf
    echo "Alias /ganglia /opt/ganglia-web" >> /etc/httpd/conf/httpd.conf
    echo "<Directory \"/opt/ganglia-web\">" >> /etc/httpd/conf/httpd.conf
    echo "       AllowOverride All" >> /etc/httpd/conf/httpd.conf
    echo "       Order allow,deny" >> /etc/httpd/conf/httpd.conf

    if [ -z "$isAmazonLinux" ]; then
        echo "       Require all granted" >> /etc/httpd/conf/httpd.conf
    fi

    echo "       Allow from all" >> /etc/httpd/conf/httpd.conf
    echo "       Deny from none" >> /etc/httpd/conf/httpd.conf
    echo "</Directory>" >> /etc/httpd/conf/httpd.conf

    echo "[INFO] ganglia-web successfully installed"
}

# Setup ntpd service
setupNTP()
{
    echo "[INFO] Installing ntp package"

    yum -y install ntp

    if [ $? -ne 0 ]; then
        terminate "Failed to install ntp package"
    fi

    echo "[INFO] Starting ntpd service"

    service ntpd restart

    if [ $? -ne 0 ]; then
        terminate "Failed to restart ntpd service"
    fi
}

# Installs and run Ganglia agent ('gmond' daemon)
bootstrapGangliaAgent()
{
    echo "[INFO]-----------------------------------------------------------------"
    echo "[INFO] Bootstrapping Ganglia agent"
    echo "[INFO]-----------------------------------------------------------------"

    installGangliaPackages

    echo "[INFO] Running ganglia agent daemon to discover Ganglia master"

    /opt/ignite-cassandra-tests/bootstrap/aws/ganglia/agent-start.sh $1 $2 > /opt/ganglia-agent.log &

    echo "[INFO] Ganglia daemon job id: $!"
}

# Partitioning, formatting to ext4 and mounting all unpartitioned drives.
# As a result env array MOUNT_POINTS provides all newly created mount points.
mountUnpartitionedDrives()
{
    MOUNT_POINTS=

    echo "[INFO] Mounting unpartitioned drives"

    lsblk -V &> /dev/null

    if [ $? -ne 0 ]; then
        echo "[WARN] lsblk utility doesn't exist"
        echo "[INFO] Installing util-linux-ng package"

        yum -y install util-linux-ng

        if [ $? -ne 0 ]; then
            terminate "Failed to install util-linux-ng package"
        fi
    fi

    parted -v &> /dev/null

    if [ $? -ne 0 ]; then
        echo "[WARN] parted utility doesn't exist"
        echo "[INFO] Installing parted package"

        yum -y install parted

        if [ $? -ne 0 ]; then
            terminate "Failed to install parted package"
        fi
    fi

    drives=$(lsblk -io KNAME,TYPE | grep disk | sed -r "s/disk//g" | xargs)

    echo "[INFO] Found HDDs: $drives"

    unpartDrives=
    partDrives=$(lsblk -io KNAME,TYPE | grep part | sed -r "s/[0-9]*//g" | sed -r "s/part//g" | xargs)

    drives=($drives)
	count=${#drives[@]}
	iter=1

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		drive=${drives[$i]}

        if [ -z "$drive" ]; then
            continue
        fi

        isPartitioned=$(echo $partDrives | grep "$drive")

        if [ -n "$isPartitioned" ]; then
            continue
        fi

        echo "[INFO] Creating partition for the drive: $drive"

        parted -s -a opt /dev/$drive mklabel gpt mkpart primary 0% 100%

        if [ $? -ne 0 ]; then
            terminate "Failed to create partition for the drive: $drive"
        fi

        partition=$(lsblk -io KNAME,TYPE | grep part | grep $drive | sed -r "s/part//g" | xargs)

        echo "[INFO] Successfully created partition $partition for the drive: $drive"

        echo "[INFO] Formatting partition /dev/$partition to ext4"

        mkfs.ext4 -F -q /dev/$partition

        if [ $? -ne 0 ]; then
            terminate "Failed to format partition: /dev/$partition"
        fi

        echo "[INFO] Partition /dev/$partition was successfully formatted to ext4"

        echo "[INFO] Mounting partition /dev/$partition to /storage$iter"

        mkdir -p /storage$iter

        if [ $? -ne 0 ]; then
            terminate "Failed to create mount point directory: /storage$iter"
        fi

        echo "/dev/$partition               /storage$iter               ext4    defaults        1 1" >> /etc/fstab

        mount /storage$iter

        if [ $? -ne 0 ]; then
            terminate "Failed to mount /storage$iter mount point for partition /dev/$partition"
        fi

        echo "[INFO] Partition /dev/$partition was successfully mounted to /storage$iter"

        if [ -n "$MOUNT_POINTS" ]; then
            MOUNT_POINTS="$MOUNT_POINTS "
        fi

        MOUNT_POINTS="${MOUNT_POINTS}/storage${iter}"

        iter=$(($iter+1))
    done

    if [ -z "$MOUNT_POINTS" ]; then
        echo "[INFO] All drives already have partitions created"
    fi

    MOUNT_POINTS=($MOUNT_POINTS)
}

# Creates storage directories for Cassandra: data files, commit log, saved caches.
# As a result CASSANDRA_DATA_DIR, CASSANDRA_COMMITLOG_DIR, CASSANDRA_CACHES_DIR will point to appropriate directories.
createCassandraStorageLayout()
{
    CASSANDRA_DATA_DIR=
    CASSANDRA_COMMITLOG_DIR=
    CASSANDRA_CACHES_DIR=

    mountUnpartitionedDrives

    echo "[INFO] Creating Cassandra storage layout"

	count=${#MOUNT_POINTS[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        mountPoint=${MOUNT_POINTS[$i]}

        if [ -z "$CASSANDRA_DATA_DIR" ]; then
            CASSANDRA_DATA_DIR=$mountPoint
        elif [ -z "$CASSANDRA_COMMITLOG_DIR" ]; then
            CASSANDRA_COMMITLOG_DIR=$mountPoint
        elif [ -z "$CASSANDRA_CACHES_DIR" ]; then
            CASSANDRA_CACHES_DIR=$mountPoint
        else
            CASSANDRA_DATA_DIR="$CASSANDRA_DATA_DIR $mountPoint"
        fi
    done

    if [ -z "$CASSANDRA_DATA_DIR" ]; then
        CASSANDRA_DATA_DIR="/storage/cassandra/data"
    else
        CASSANDRA_DATA_DIR="$CASSANDRA_DATA_DIR/cassandra_data"
    fi

    if [ -z "$CASSANDRA_COMMITLOG_DIR" ]; then
        CASSANDRA_COMMITLOG_DIR="/storage/cassandra/commitlog"
    else
        CASSANDRA_COMMITLOG_DIR="$CASSANDRA_COMMITLOG_DIR/cassandra_commitlog"
    fi

    if [ -z "$CASSANDRA_CACHES_DIR" ]; then
        CASSANDRA_CACHES_DIR="/storage/cassandra/saved_caches"
    else
        CASSANDRA_CACHES_DIR="$CASSANDRA_CACHES_DIR/cassandra_caches"
    fi

    echo "[INFO] Cassandra data dir: $CASSANDRA_DATA_DIR"
    echo "[INFO] Cassandra commit log dir: $CASSANDRA_COMMITLOG_DIR"
    echo "[INFO] Cassandra saved caches dir: $CASSANDRA_CACHES_DIR"

    dirs=("$CASSANDRA_DATA_DIR $CASSANDRA_COMMITLOG_DIR $CASSANDRA_CACHES_DIR")

	count=${#dirs[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        directory=${dirs[$i]}

        mkdir -p $directory

        if [ $? -ne 0 ]; then
            terminate "Failed to create directory: $directory"
        fi

        chown -R cassandra:cassandra $directory

        if [ $? -ne 0 ]; then
            terminate "Failed to assign cassandra:cassandra as an owner of directory $directory"
        fi
    done

    DATA_DIR_SPEC="\n"

    dirs=($CASSANDRA_DATA_DIR)

	count=${#dirs[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
    do
        dataDir=${dirs[$i]}
        DATA_DIR_SPEC="${DATA_DIR_SPEC}     - ${dataDir}\n"
    done

    CASSANDRA_DATA_DIR=$(echo $DATA_DIR_SPEC | sed -r "s/\//\\\\\//g")
    CASSANDRA_COMMITLOG_DIR=$(echo $CASSANDRA_COMMITLOG_DIR | sed -r "s/\//\\\\\//g")
    CASSANDRA_CACHES_DIR=$(echo $CASSANDRA_CACHES_DIR | sed -r "s/\//\\\\\//g")
}

# Attaches environment configuration settings
. $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/env.sh

# Validates environment settings
validate

# Validates node type of EC2 instance
if [ "$1" != "cassandra" ] && [ "$1" != "ignite" ] && [ "$1" != "test" ] && [ "$1" != "ganglia" ]; then
    echo "[ERROR] Unsupported node type specified: $1"
    exit 1
fi

# Sets node type of EC2 instance
export NODE_TYPE=$1
