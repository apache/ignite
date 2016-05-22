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
    if [[ "$S3_TESTS_SUCCESS_URL" != */ ]]; then
        S3_TESTS_SUCCESS_URL=${S3_TESTS_SUCCESS_URL}/
    fi

    if [[ "$S3_TESTS_FAILURE_URL" != */ ]]; then
        S3_TESTS_FAILURE_URL=${S3_TESTS_FAILURE_URL}/
    fi

    msg=$HOST_NAME

    msg=$1
    reportFolder=${S3_TESTS_FAILURE_URL}${HOST_NAME}
    reportFile=$reportFolder/__error__

    echo $msg > /opt/ignite-cassandra-tests/tests-result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed drop report folder: $reportFolder"
    fi

    if [ -d "/opt/ignite-cassandra-tests/logs" ]; then
        aws s3 sync --sse AES256 /opt/ignite-cassandra-tests/logs $reportFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export tests logs to: $reportFolder"
        fi
    fi

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/tests-result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to report tests results to: $reportFile"
    fi

    aws s3 rm ${S3_TESTS_RUNNING_URL}${HOST_NAME}
    aws s3 rm ${S3_TESTS_WAITING_URL}${HOST_NAME}
    aws s3 rm ${S3_TESTS_IDLE_URL}${HOST_NAME}
    aws s3 rm ${S3_TESTS_PREPARING_URL}${HOST_NAME}
    aws s3 rm --recursive ${S3_TESTS_SUCCESS_URL}${HOST_NAME}

    if [ "$FIRST_NODE" == "true" ]; then
        removeFirstNodeLock
    fi

    rm -Rf /opt/ignite-cassandra-tests/tests-result /opt/ignite-cassandra-tests/hostname

    unregisterNode

    exit 1
}

switchToIdleState()
{
    if [ "$NODE_STATE" != "IDLE" ]; then
        echo "[INFO] Switching node to IDLE state"
        dropStateFlag "$S3_TESTS_WAITING_URL" "$S3_TESTS_PREPARING_URL" "$S3_TESTS_RUNNING_URL"
        createStateFlag "$S3_TESTS_IDLE_URL"
        NODE_STATE="IDLE"
        echo "[INFO] Node was switched to IDLE state"
    fi
}

switchToPreparingState()
{
    if [ "$NODE_STATE" != "PREPARING" ]; then
        echo "[INFO] Switching node to PREPARING state"
        dropStateFlag "$S3_TESTS_WAITING_URL" "$S3_TESTS_IDLE_URL" "$S3_TESTS_RUNNING_URL"
        createStateFlag "$S3_TESTS_PREPARING_URL"
        NODE_STATE="PREPARING"
        echo "[INFO] Node was switched to PREPARING state"
    fi
}

switchToWaitingState()
{
    if [ "$NODE_STATE" != "WAITING" ]; then
        echo "[INFO] Switching node to WAITING state"
        dropStateFlag "$S3_TESTS_IDLE_URL" "$S3_TESTS_PREPARING_URL" "$S3_TESTS_RUNNING_URL"
        createStateFlag "$S3_TESTS_WAITING_URL"
        NODE_STATE="WAITING"
        echo "[INFO] Node was switched to WAITING state"
    fi
}

switchToRunningState()
{
    if [ "$NODE_STATE" != "RUNNING" ]; then
        echo "[INFO] Switching node to RUNNING state"
        dropStateFlag "$S3_TESTS_IDLE_URL" "$S3_TESTS_PREPARING_URL" "$S3_TESTS_WAITING_URL"
        createStateFlag "$S3_TESTS_RUNNING_URL"
        NODE_STATE="RUNNING"
        echo "[INFO] Node was switched to RUNNING state"
    fi
}

createStateFlag()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname ${1}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to create state flag: ${1}${HOST_NAME}"
    fi
}

dropStateFlag()
{
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

dropTestsSummary()
{
    exists=$(aws s3 ls $S3_TESTS_SUMMARY_URL)
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm $S3_TESTS_SUMMARY_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to drop tests summary info: $S3_TESTS_SUMMARY_URL"
    fi
}

recreateCassandraArtifacts()
{
    /opt/ignite-cassandra-tests/recreate-cassandra-artifacts.sh
    if [ $? -ne 0 ]; then
        terminate "Failed to recreate Cassandra artifacts"
    fi
}

validate()
{
    if [ -z "$TESTS_TYPE" ]; then
        terminate "Tests type 'ignite' or 'cassandra' should be specified"
    fi

    if [ "$TESTS_TYPE" != "ignite" ] && [ "$TESTS_TYPE" != "cassandra" ]; then
        terminate "Incorrect tests type specified: $TESTS_TYPE"
    fi

    if [ -z "$S3_TESTS_SUCCESS_URL" ]; then
        terminate "Tests success URL doesn't specified"
    fi

    if [[ "$S3_TESTS_SUCCESS_URL" != */ ]]; then
        S3_TESTS_SUCCESS_URL=${S3_TESTS_SUCCESS_URL}/
    fi

    if [ -z "$S3_TESTS_FAILURE_URL" ]; then
        terminate "Tests failure URL doesn't specified"
    fi

    if [[ "$S3_TESTS_FAILURE_URL" != */ ]]; then
        S3_TESTS_FAILURE_URL=${S3_TESTS_FAILURE_URL}/
    fi

    if [ -z "$S3_TESTS_IDLE_URL" ]; then
        terminate "Tests idle URL doesn't specified"
    fi

    if [[ "$S3_TESTS_IDLE_URL" != */ ]]; then
        S3_TESTS_IDLE_URL=${S3_TESTS_IDLE_URL}/
    fi

    if [ -z "$S3_TESTS_PREPARING_URL" ]; then
        terminate "Tests preparing URL doesn't specified"
    fi

    if [[ "$S3_TESTS_PREPARING_URL" != */ ]]; then
        S3_TESTS_PREPARING_URL=${S3_TESTS_PREPARING_URL}/
    fi

    if [ -z "$S3_TESTS_RUNNING_URL" ]; then
        terminate "Tests running URL doesn't specified"
    fi

    if [[ "$S3_TESTS_RUNNING_URL" != */ ]]; then
        S3_TESTS_RUNNING_URL=${S3_TESTS_RUNNING_URL}/
    fi

    if [ -z "$S3_TESTS_WAITING_URL" ]; then
        terminate "Tests waiting URL doesn't specified"
    fi

    if [[ "$S3_TESTS_WAITING_URL" != */ ]]; then
        S3_TESTS_WAITING_URL=${S3_TESTS_WAITING_URL}/
    fi

    if [ -z "$S3_IGNITE_SUCCESS_URL" ]; then
        terminate "Ignite success URL doesn't specified"
    fi

    if [[ "$S3_IGNITE_SUCCESS_URL" != */ ]]; then
        S3_IGNITE_SUCCESS_URL=${S3_IGNITE_SUCCESS_URL}/
    fi

    if [ -z "$S3_IGNITE_FAILURE_URL" ]; then
        terminate "Ignite failure URL doesn't specified"
    fi

    if [[ "$S3_IGNITE_FAILURE_URL" != */ ]]; then
        S3_IGNITE_FAILURE_URL=${S3_IGNITE_FAILURE_URL}/
    fi

    if [ -z "$S3_CASSANDRA_SUCCESS_URL" ]; then
        terminate "Cassandra success URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_SUCCESS_URL" != */ ]]; then
        S3_CASSANDRA_SUCCESS_URL=${S3_CASSANDRA_SUCCESS_URL}/
    fi

    if [ -z "$S3_CASSANDRA_FAILURE_URL" ]; then
        terminate "Cassandra failure URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_FAILURE_URL" != */ ]]; then
        S3_CASSANDRA_FAILURE_URL=${S3_CASSANDRA_FAILURE_URL}/
    fi

    if [ -z "$S3_CASSANDRA_NODES_DISCOVERY_URL" ]; then
        terminate "Cassandra S3 discovery URL doesn't specified"
    fi

    if [[ "$S3_CASSANDRA_NODES_DISCOVERY_URL" != */ ]]; then
        S3_CASSANDRA_NODES_DISCOVERY_URL=${S3_CASSANDRA_NODES_DISCOVERY_URL}/
    fi

    if [ -z "$S3_IGNITE_NODES_DISCOVERY_URL" ]; then
        terminate "Ignite S3 discovery URL doesn't specified"
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

    if [ -z "$S3_TEST_NODES_DISCOVERY_URL" ]; then
        terminate "Tests S3 discovery URL doesn't specified"
    fi

    if [[ "$S3_TEST_NODES_DISCOVERY_URL" != */ ]]; then
        S3_TEST_NODES_DISCOVERY_URL=${S3_TEST_NODES_DISCOVERY_URL}/
    fi
}

setupCassandraSeeds()
{
    if [ $CASSANDRA_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    CASSANDRA_SEEDS1=
    CASSANDRA_SEEDS2=

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

            CASSANDRA_SEEDS1="<value>$seed1<\/value>"
            CASSANDRA_SEEDS2="$seed1"

            if [ "$seed2" != "$seed1" ] && [ -n "$seed2" ]; then
                CASSANDRA_SEEDS1="$CASSANDRA_SEEDS1<value>$seed2<\/value>"
                CASSANDRA_SEEDS2="${CASSANDRA_SEEDS2},$seed2"
            fi

            if [ "$seed3" != "$seed2" ] && [ "$seed3" != "$seed1" ] && [ -n "$seed3" ]; then
                CASSANDRA_SEEDS1="$CASSANDRA_SEEDS1<value>$seed3<\/value>"
                CASSANDRA_SEEDS2="${CASSANDRA_SEEDS2},$seed3"
            fi

            echo "[INFO] Using Cassandra seeds: $CASSANDRA_SEEDS2"

            echo "contact.points=$CASSANDRA_SEEDS2" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/connection.properties

            cat /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template.xml | sed -r "s/\\\$\{CASSANDRA_SEEDS\}/$CASSANDRA_SEEDS1/g" > /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml

            return 0
        fi

        echo "[INFO] Waiting for the first Cassandra node to start and publish its seed, time passed ${duration}min"

        sleep 30s
    done
}

setupIgniteSeeds()
{
    if [ $IGNITE_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    echo "[INFO] Setting up Ignite seeds"

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

            cat /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml | sed -r "s/\\\$\{IGNITE_SEEDS\}/$IGNITE_SEEDS/g" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/persistence/primitive/ignite-remote-client-config.xml
            rm -f /opt/ignite-cassandra-tests/bootstrap/aws/tests/ignite-cassandra-client-template1.xml

            return 0
        fi

        currentTime=$(date +%s)
        duration=$(( $currentTime-$startTime ))
        duration=$(( $duration/60 ))

        echo "[INFO] Waiting for the first Ignite node to start and publish its seed, time passed ${duration}min"

        sleep 30s
    done
}

tryToGetFirstNodeLock()
{
    if [ "$FIRST_NODE" == "true" ]; then
        return 0
    fi

    echo "[INFO] Trying to get first node lock"

    checkFirstNodeLockExist
    if [ $? -ne 0 ]; then
        return 1
    fi

    createFirstNodeLock

    sleep 5s

    rm -Rf /opt/ignite-cassandra-tests/first-node-lock

    aws s3 cp $S3_TESTS_FIRST_NODE_LOCK_URL /opt/ignite-cassandra-tests/first-node-lock
    if [ $? -ne 0 ]; then
        echo "[WARN] Failed to check just created first node lock"
        return 1
    fi

    first_host=$(cat /opt/ignite-cassandra-tests/first-node-lock)

    rm -f /opt/ignite-cassandra-tests/first-node-lock

    if [ "$first_host" != "$HOST_NAME" ]; then
        echo "[INFO] Node $first_host has discarded previously created first node lock"
        return 1
    fi

    echo "[INFO] Congratulations, got first node lock"

    FIRST_NODE="true"

    return 0
}

checkFirstNodeLockExist()
{
    echo "[INFO] Checking for the first node lock"

    lockExists=$(aws s3 ls $S3_TESTS_FIRST_NODE_LOCK_URL)
    if [ -n "$lockExists" ]; then
        echo "[INFO] First node lock already exists"
        return 1
    fi

    echo "[INFO] First node lock doesn't exist yet"

    return 0
}

createFirstNodeLock()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname $S3_TESTS_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        terminate "Failed to create first node lock"
    fi
    echo "[INFO] Created first node lock"
}

removeFirstNodeLock()
{
    if [ "$FIRST_NODE" != "true" ]; then
        return 0
    fi

    exists=$(aws s3 ls $S3_TESTS_FIRST_NODE_LOCK_URL)
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm $S3_TESTS_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to remove first node lock"
        return 1
    fi

    FIRST_NODE="false"

    echo "[INFO] Removed first node lock"
}

waitAllIgniteNodesReady()
{
    if [ $IGNITE_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    echo "[INFO] Waiting for all $IGNITE_NODES_COUNT Ignite nodes up and running"

    while true; do
        successCount=$(aws s3 ls $S3_IGNITE_SUCCESS_URL | wc -l)

        if [ $successCount -ge $IGNITE_NODES_COUNT ]; then
            break
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    echo "[INFO] Congratulation, all $IGNITE_NODES_COUNT Ignite nodes are up and running"
}

waitAllCassandraNodesReady()
{
    if [ $CASSANDRA_NODES_COUNT -eq 0 ]; then
        return 0
    fi

    echo "[INFO] Waiting for all $CASSANDRA_NODES_COUNT Cassandra nodes up and running"

    while true; do
        successCount=$(aws s3 ls $S3_CASSANDRA_SUCCESS_URL | wc -l)

        if [ $successCount -ge $CASSANDRA_NODES_COUNT ]; then
            break
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    echo "[INFO] Congratulation, all $CASSANDRA_NODES_COUNT Cassandra nodes are up and running"
}

waitAllTestNodesReadyToRunTests()
{
    echo "[INFO] Waiting for all $TEST_NODES_COUNT test nodes ready to run tests"

    while true; do

        count1=$(aws s3 ls $S3_TESTS_WAITING_URL | wc -l)
        count2=$(aws s3 ls $S3_TESTS_RUNNING_URL | wc -l)

        count=$(( $count1+$count2 ))

        if [ $count -ge $TEST_NODES_COUNT ]; then
            break
        fi

        echo "[INFO] $count nodes are ready to run tests, waiting for other nodes for extra 30sec"

        sleep 30s
    done

    sleep 1m

    echo "[INFO] Congratulation, all $TEST_NODES_COUNT test nodes are ready to run tests"
}

waitAllTestNodesCompletedTests()
{
    echo "[INFO] Waiting for all $TEST_NODES_COUNT test nodes to complete their tests"

    while true; do

        nodesCount=$(aws s3 ls $S3_TESTS_RUNNING_URL | grep -v $HOST_NAME | wc -l)

        if [ $nodesCount -eq 0 ]; then
            break
        fi

        echo "[INFO] Waiting extra 30sec"

        sleep 30s
    done

    echo "[INFO] Congratulation, all $TEST_NODES_COUNT test nodes have completed their tests"
}

printTestsInfo()
{
    echo "[INFO]-----------------------------------------------------------------"
    echo "[INFO] Test nodes count: $TEST_NODES_COUNT"
    echo "[INFO] Ignite nodes count: $IGNITE_NODES_COUNT"
    echo "[INFO] Cassandra nodes count: $CASSANDRA_NODES_COUNT"
    echo "[INFO] Tests summary URL: $S3_TESTS_SUMMARY_URL"
    echo "[INFO] Tests first node lock URL: $S3_TESTS_FIRST_NODE_LOCK_URL"
    echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
    echo "[INFO] Test node discovery URL: $S3_TEST_NODES_DISCOVERY_URL"
    echo "[INFO] Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY_URL"
    echo "[INFO] Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
    echo "[INFO] Tests idle URL: $S3_TESTS_IDLE_URL"
    echo "[INFO] Tests preparing URL: $S3_TESTS_PREPARING_URL"
    echo "[INFO] Tests waiting URL: $S3_TESTS_WAITING_URL"
    echo "[INFO] Tests running URL: $S3_TESTS_RUNNING_URL"
    echo "[INFO] Tests success URL: $S3_TESTS_SUCCESS_URL"
    echo "[INFO] Tests failure URL: $S3_TESTS_FAILURE_URL"
    echo "[INFO] Ignite success URL: $S3_IGNITE_SUCCESS_URL"
    echo "[INFO] Ignite failure URL: $S3_IGNITE_FAILURE_URL"
    echo "[INFO] Cassandra success URL: $S3_CASSANDRA_SUCCESS_URL"
    echo "[INFO] Cassandra failure URL: $S3_CASSANDRA_FAILURE_URL"
    echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER_URL"
    echo "[INFO] Tests trigger URL: $S3_TESTS_TRIGGER_URL"
    echo "[INFO] JAVA_HOME: $JAVA_HOME"
    echo "[INFO] PATH: $PATH"
    echo "[INFO]-----------------------------------------------------------------"
}

setupCassandraCredentials()
{
    echo "admin.user=cassandra" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "admin.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "regular.user=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
    echo "regular.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
}

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
    echo "--------------------------------------------------" >> /opt/ignite-cassandra-tests/tests-trigger
    echo "" >> /opt/ignite-cassandra-tests/tests-trigger
    cat /opt/ignite-cassandra-tests/settings/tests.properties >> /opt/ignite-cassandra-tests/tests-trigger

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/tests-trigger $S3_TESTS_TRIGGER_URL
    code=$?

    rm -f /opt/ignite-cassandra-tests/tests-trigger

    if [ $code -ne 0 ]; then
        terminate "Failed to create tests trigger: $S3_TESTS_TRIGGER_URL"
    fi
}

cleanPreviousLogs()
{
	for logFile in /opt/ignite-cassandra-tests/logs/*
	do
	    managerLog=$(echo $logFile | grep "tests-manager")
	    if [ -z "$managerLog" ]; then
	        rm -Rf $logFile
	    fi
	done

	aws s3 rm --recursive ${S3_TESTS_FAILURE_URL}${HOST_NAME}
	aws s3 rm --recursive ${S3_TESTS_SUCCESS_URL}${HOST_NAME}
}

uploadTestsLogs()
{
    if [ -f "/opt/ignite-cassandra-tests/logs/__success__" ]; then
        logsFolder=${S3_TESTS_SUCCESS_URL}${HOST_NAME}
    else
        logsFolder=${S3_TESTS_FAILURE_URL}${HOST_NAME}
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

buildTestsSummaryReport()
{
    reportScript=$(readlink -m $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/tests-report.sh)
    $reportScript

    if [ -n "$S3_LOGS_TRIGGER_URL" ]; then
        aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname $S3_LOGS_TRIGGER_URL
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to trigger logs collection"
        fi
    fi
}


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

    cp -f /opt/ignite-cassandra-tests/settings/tests.properties /opt/ignite-cassandra-tests/logs

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

registerNode()
{
    echo "[INFO] Registering Test node seed: ${S3_TEST_NODES_DISCOVERY_URL}$HOST_NAME"

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname ${S3_TEST_NODES_DISCOVERY_URL}$HOST_NAME
    if [ $? -ne 0 ]; then
        terminate "Failed to register Test node seed: ${S3_TEST_NODES_DISCOVERY_URL}$HOST_NAME"
    fi

    echo "[INFO] Test node successfully registered"
}

unregisterNode()
{
    echo "[INFO] Removing Test node registration from: ${S3_TEST_NODES_DISCOVERY_URL}$HOST_NAME"
    aws s3 rm ${S3_TEST_NODES_DISCOVERY_URL}$HOST_NAME
    echo "[INFO] Test node registration removed"
}


sleep 1m

HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
echo $HOST_NAME > /opt/ignite-cassandra-tests/hostname

FIRST_NODE="false"
NODE_STATE=
TRIGGER_STATE=

validate
printTestsInfo
setupCassandraCredentials
switchToIdleState

triggerFirstTimeTestsExecution

registerNode

while true; do
    # switching state to IDLE
    switchToIdleState

    sleep 30s

    NEW_TRIGGER_STATE=$(aws s3 ls $S3_TESTS_TRIGGER_URL | xargs)
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

    aws s3 cp $S3_TESTS_TRIGGER_URL /opt/ignite-cassandra-tests/tests-trigger
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to download tests trigger info from: $S3_TESTS_TRIGGER_URL"
        continue
    fi

    TESTS_TYPE=$(cat /opt/ignite-cassandra-tests/tests-trigger | grep TESTS_TYPE | xargs | sed -r "s/TESTS_TYPE=//g")
    if [ "$TESTS_TYPE" != "ignite" ] && [ "$TESTS_TYPE" != "cassandra" ]; then
        rm -f /opt/ignite-cassandra-tests/tests-trigger
        echo "[ERROR] Incorrect tests type specified in the trigger info: $S3_TESTS_TRIGGER_URL"
        continue
    fi

    rm -f /opt/ignite-cassandra-tests/settings/tests.properties
    mv -f /opt/ignite-cassandra-tests/tests-trigger /opt/ignite-cassandra-tests/settings/tests.properties
	
	waitAllTestNodesCompletedTests
	
    # switching state to PREPARING
    switchToPreparingState

    waitAllCassandraNodesReady
    waitAllIgniteNodesReady
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

    waitAllTestNodesReadyToRunTests
	
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