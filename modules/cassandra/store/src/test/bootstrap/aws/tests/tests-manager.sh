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
# Tests manager daemon
# -----------------------------------------------------------------------------------------------
# Script is launched in background by all nodes of Tests cluster and
# periodically (each 30 seconds) checks if specific S3 trigger file was created or
# its timestamp was changed. Such an event serve as a trigger for the script to start
# preparing to run load tests.
# -----------------------------------------------------------------------------------------------

#profile=/home/ignite/.bash_profile
profile=/root/.bash_profile

. $profile
. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "test"

# Switch test node to IDLE state
switchToIdleState()
{
    if [ "$NODE_STATE" != "IDLE" ]; then
        echo "[INFO] Switching node to IDLE state"
        dropStateFlag "$S3_TESTS_WAITING" "$S3_TESTS_PREPARING" "$S3_TESTS_RUNNING"
        createStateFlag "$S3_TESTS_IDLE"
        NODE_STATE="IDLE"
        echo "[INFO] Node was switched to IDLE state"
    fi
}

# Switch test node to PREPARING state
switchToPreparingState()
{
    if [ "$NODE_STATE" != "PREPARING" ]; then
        echo "[INFO] Switching node to PREPARING state"
        dropStateFlag "$S3_TESTS_WAITING" "$S3_TESTS_IDLE" "$S3_TESTS_RUNNING"
        createStateFlag "$S3_TESTS_PREPARING"
        NODE_STATE="PREPARING"
        echo "[INFO] Node was switched to PREPARING state"
    fi
}

# Switch test node to WAITING state
switchToWaitingState()
{
    if [ "$NODE_STATE" != "WAITING" ]; then
        echo "[INFO] Switching node to WAITING state"
        dropStateFlag "$S3_TESTS_IDLE" "$S3_TESTS_PREPARING" "$S3_TESTS_RUNNING"
        createStateFlag "$S3_TESTS_WAITING"
        NODE_STATE="WAITING"
        echo "[INFO] Node was switched to WAITING state"
    fi
}

# Switch test node to RUNNING state
switchToRunningState()
{
    if [ "$NODE_STATE" != "RUNNING" ]; then
        echo "[INFO] Switching node to RUNNING state"
        dropStateFlag "$S3_TESTS_IDLE" "$S3_TESTS_PREPARING" "$S3_TESTS_WAITING"
        createStateFlag "$S3_TESTS_RUNNING"
        NODE_STATE="RUNNING"
        echo "[INFO] Node was switched to RUNNING state"
    fi
}

# Creates appropriate state flag for the node in S3
createStateFlag()
{
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    aws s3 cp --sse AES256 /etc/hosts ${1}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to create state flag: ${1}${HOST_NAME}"
    fi
}

# Drops appropriate state flag for the node in S3
dropStateFlag()
{
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    for flagUrl in "$@"
    do
        exists=$(aws s3 ls ${flagUrl}${HOST_NAME})
        if [ -n "$exists" ]; then
            aws s3 rm ${flagUrl}${HOST_NAME}
            if [ $? -ne 0 ]; then
                terminate "Failed to drop state flag: ${flagUrl}${HOST_NAME}"
            fi
        fi
    done
}

# Removes tests summary report from S3
dropTestsSummary()
{
    exists=$(aws s3 ls $S3_TESTS_SUMMARY)
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm $S3_TESTS_SUMMARY
    if [ $? -ne 0 ]; then
        terminate "Failed to drop tests summary info: $S3_TESTS_SUMMARY"
    fi
}

# Recreate all the necessary Cassandra artifacts before running Load tests
recreateCassandraArtifacts()
{
    /opt/ignite-cassandra-tests/recreate-cassandra-artifacts.sh
    if [ $? -ne 0 ]; then
        terminate "Failed to recreate Cassandra artifacts"
    fi
}

# Setups Cassandra seeds for this Tests node being able to connect to Cassandra.
# Looks for the information in S3 about already up and running Cassandra cluster nodes.
setupCassandraSeeds()
{
    if [ $CASSANDRA_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    setupClusterSeeds "cassandra"

    CASSANDRA_SEEDS1=$(echo $CLUSTER_SEEDS | sed -r "s/ /,/g")
    CASSANDRA_SEEDS2=

    CLUSTER_SEEDS=($CLUSTER_SEEDS)
	count=${#CLUSTER_SEEDS[@]}

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		seed=${CLUSTER_SEEDS[$i]}
        CASSANDRA_SEEDS2="${CASSANDRA_SEEDS2}<value>$seed<\/value>"
	done

    echo "[INFO] Using Cassandra seeds: $CASSANDRA_SEEDS1"

    echo "contact.points=$CASSANDRA_SEEDS1" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/connection.properties

    cat /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template.xml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS2/g" > /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml
}

# Setups Ignite nodes for this Tests node being able to connect to Ignite.
# Looks for the information in S3 about already up and running Cassandra cluster nodes.
setupIgniteSeeds()
{
    if [ $IGNITE_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    setupClusterSeeds "ignite"

    CLUSTER_SEEDS=($CLUSTER_SEEDS)
	count=${#CLUSTER_SEEDS[@]}

    IGNITE_SEEDS=

	for (( i=0; i<=$(( $count -1 )); i++ ))
	do
		seed=${CLUSTER_SEEDS[$i]}
        IGNITE_SEEDS="${IGNITE_SEEDS}<value>$seed<\/value>"
	done

    echo "[INFO] Using Ignite seeds: $IGNITE_SEEDS"

    cat /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml | sed -r "s/\\\$\{IGNITE_SEEDS\}/$IGNITE_SEEDS/g" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/persistence/primitive/ignite-remote-client-config.xml
    rm -f /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml
}

# Setups Cassandra credentials to connect to Cassandra cluster
setupCassandraCredentials()
{
    echo "admin.user=cassandra" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "admin.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "regular.user=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "regular.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
}

# Triggering first time tests execution for all nodes in the Tests cluster
triggerFirstTimeTestsExecution()
{
    if [ -z "$TESTS_TYPE" ]; then
        return 0
    fi

    tryToGetFirstNodeLock
    if [ $? -ne 0 ]; then
        return 0
    fi

    sleep 30s

    echo "[INFO] Triggering first time tests execution"

    echo "TESTS_TYPE=$TESTS_TYPE" > /opt/ignite-cassandra-tests/tests-trigger
    echo "#--------------------------------------------------" >> /opt/ignite-cassandra-tests/tests-trigger
    echo "" >> /opt/ignite-cassandra-tests/tests-trigger
    cat /opt/ignite-cassandra-tests/settings/tests.properties >> /opt/ignite-cassandra-tests/tests-trigger

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/tests-trigger $S3_TESTS_TRIGGER
    code=$?

    rm -f /opt/ignite-cassandra-tests/tests-trigger

    if [ $code -ne 0 ]; then
        terminate "Failed to create tests trigger: $S3_TESTS_TRIGGER"
    fi
}

# Cleans previously created logs from S3
cleanPreviousLogs()
{
	for logFile in /opt/ignite-cassandra-tests/logs/*
	do
	    managerLog=$(echo $logFile | grep "tests-manager")
	    if [ -z "$managerLog" ]; then
	        rm -Rf $logFile
	    fi
	done

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

	aws s3 rm --recursive ${S3_TESTS_FAILURE}${HOST_NAME}
	aws s3 rm --recursive ${S3_TESTS_SUCCESS}${HOST_NAME}
}

# Uploads tests logs to S3
uploadTestsLogs()
{
    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    if [ -f "/opt/ignite-cassandra-tests/logs/__success__" ]; then
        logsFolder=${S3_TESTS_SUCCESS}${HOST_NAME}
    else
        logsFolder=${S3_TESTS_FAILURE}${HOST_NAME}
    fi

    aws s3 rm --recursive $logsFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to drop logs folder: $logsFolder"
    fi

    if [ -d "/opt/ignite-cassandra-tests/logs" ]; then
        aws s3 sync --sse AES256 /opt/ignite-cassandra-tests/logs $logsFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export tests logs to: $logsFolder"
        fi
    fi
}

# Runs tests-report.sh to prepare tests summary report
buildTestsSummaryReport()
{
    reportScript=$(readlink -m $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/tests-report.sh)
    $reportScript

    if [ -n "$S3_LOGS_TRIGGER" ]; then
        aws s3 cp --sse AES256 /etc/hosts $S3_LOGS_TRIGGER
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to trigger logs collection"
        fi
    fi
}

# Running load tests
runLoadTests()
{
    cd /opt/ignite-cassandra-tests

    if [ "$TESTS_TYPE" == "ignite" ]; then
        echo "[INFO] Running Ignite load tests"
        ./ignite-load-tests.sh &
    else
        echo "[INFO] Running Cassandra load tests"
        ./cassandra-load-tests.sh &
    fi

    testsJobId=$!

    echo "[INFO] Tests job id: $testsJobId"

    sleep 1m

    LOGS_SNAPSHOT=$(ls -al /opt/ignite-cassandra-tests/logs)
    LOGS_SNAPSHOT_TIME=$(date +%s)

    TERMINATED=

    # tests monitoring
    while true; do
        proc=$(ps -ef | grep java | grep "org.apache.ignite.tests")
        if [ -z "$proc" ]; then
            break
        fi

        NEW_LOGS_SNAPSHOT=$(ls -al /opt/ignite-cassandra-tests/logs)
        NEW_LOGS_SNAPSHOT_TIME=$(date +%s)

        # if logs state updated it means that tests are running and not stuck
        if [ "$LOGS_SNAPSHOT" != "$NEW_LOGS_SNAPSHOT" ]; then
            LOGS_SNAPSHOT=$NEW_LOGS_SNAPSHOT
            LOGS_SNAPSHOT_TIME=$NEW_LOGS_SNAPSHOT_TIME
            continue
        fi

        duration=$(( $NEW_LOGS_SNAPSHOT_TIME-$LOGS_SNAPSHOT_TIME ))
        duration=$(( $duration/60 ))

        # if logs wasn't updated during 5min it means that load tests stuck
        if [ $duration -gt 5 ]; then
            proc=($proc)
            kill -9 ${proc[1]}
            TERMINATED="true"
            break
        fi

        echo "[INFO] Waiting extra 30sec for load tests to complete"

        sleep 30s
    done

    rm -f /opt/ignite-cassandra-tests/logs/tests.properties
    cp /opt/ignite-cassandra-tests/settings/tests.properties /opt/ignite-cassandra-tests/logs

    if [ "$TERMINATED" == "true" ]; then
        echo "[ERROR] Load tests stuck, tests process terminated"
        echo "Load tests stuck, tests process terminated" > /opt/ignite-cassandra-tests/logs/__error__
        return 0
    fi

    failed=
    if [ "$TESTS_TYPE" == "cassandra" ]; then
        failed=$(cat /opt/ignite-cassandra-tests/cassandra-load-tests.log | grep "load tests execution failed")
    else
        failed=$(cat /opt/ignite-cassandra-tests/ignite-load-tests.log | grep "load tests execution failed")
    fi

    if [ -n "$failed" ]; then
        echo "[ERROR] Load tests execution failed"
        echo "Load tests execution failed" > /opt/ignite-cassandra-tests/logs/__error__
    else
        echo "[INFO] Load tests execution successfully completed"
        echo "Load tests execution successfully completed" > /opt/ignite-cassandra-tests/logs/__success__
    fi
}

#######################################################################################################

sleep 1m

NODE_STATE=
TRIGGER_STATE=

printInstanceInfo
setupCassandraCredentials
switchToIdleState

triggerFirstTimeTestsExecution

registerNode

while true; do
    # switching state to IDLE
    switchToIdleState

    sleep 30s

    NEW_TRIGGER_STATE=$(aws s3 ls $S3_TESTS_TRIGGER | xargs)
    if [ -z "$NEW_TRIGGER_STATE" ] || [ "$NEW_TRIGGER_STATE" == "$TRIGGER_STATE" ]; then
        continue
    fi

    echo "----------------------------------------------------------------------"
    echo "[INFO] Tests trigger changed"
    echo "----------------------------------------------------------------------"
    echo "[INFO] Old trigger: $TRIGGER_STATE"
    echo "----------------------------------------------------------------------"
    echo "[INFO] New trigger: $NEW_TRIGGER_STATE"
    echo "----------------------------------------------------------------------"

    TRIGGER_STATE=$NEW_TRIGGER_STATE

    aws s3 cp $S3_TESTS_TRIGGER /opt/ignite-cassandra-tests/tests-trigger
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to download tests trigger info from: $S3_TESTS_TRIGGER"
        continue
    fi

    TESTS_TYPE=$(cat /opt/ignite-cassandra-tests/tests-trigger | grep TESTS_TYPE | xargs | sed -r "s/TESTS_TYPE=//g")
    if [ "$TESTS_TYPE" != "ignite" ] && [ "$TESTS_TYPE" != "cassandra" ]; then
        rm -f /opt/ignite-cassandra-tests/tests-trigger
        echo "[ERROR] Incorrect tests type specified in the trigger info: $S3_TESTS_TRIGGER"
        continue
    fi

    rm -f /opt/ignite-cassandra-tests/settings/tests.properties
    mv -f /opt/ignite-cassandra-tests/tests-trigger /opt/ignite-cassandra-tests/settings/tests.properties
	
	waitAllTestNodesCompletedTests
	
    # switching state to PREPARING
    switchToPreparingState

    waitAllClusterNodesReady "cassandra"
    waitAllClusterNodesReady "ignite"
    setupCassandraSeeds
    setupIgniteSeeds
	
	cleanPreviousLogs

    tryToGetFirstNodeLock
    if [ $? -eq 0 ]; then
        dropTestsSummary
        recreateCassandraArtifacts
    fi

    # switching state to WAITING
    switchToWaitingState

    waitAllClusterNodesReady "test"

    if [ "$FIRST_NODE_LOCK" == "true" ]; then
        aws s3 rm $S3_TESTS_TRIGGER
    fi

    # switching state to RUNNING
    switchToRunningState

    runLoadTests
    uploadTestsLogs

    tryToGetFirstNodeLock
    if [ $? -eq 0 ]; then
        waitAllTestNodesCompletedTests
        buildTestsSummaryReport
        removeFirstNodeLock
    fi
done