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

    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Tests execution failed"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1
        reportFolder=${S3_TESTS_FAILURE_URL}${HOST_NAME}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Tests execution successfully completed"
        echo "[INFO]-----------------------------------------------------"
        reportFolder=${S3_TESTS_SUCCESS_URL}${HOST_NAME}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/ignite-cassandra-tests/tests-result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed drop report folder: $reportFolder"
    fi

    if [ -d "/opt/$TESTS_PACKAGE_UNZIP_DIR/logs" ]; then
        aws s3 sync --sse AES256 /opt/$TESTS_PACKAGE_UNZIP_DIR/logs $reportFolder
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to export tests logs to: $reportFolder"
        fi
    fi

    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/tests-result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to report tests results to: $reportFile"
    fi

    rm -f /opt/ignite-cassandra-tests/tests-result /opt/ignite-cassandra-tests/hostname

    aws s3 rm ${S3_TESTS_RUNNING_URL}${HOST_NAME}
    aws s3 rm ${S3_TESTS_WAITING_URL}${HOST_NAME}

    if [ "$FIRST_NODE" == "true" ]; then
        waitAllTestNodesCompleted
        removeFirstNodeLock
        reportTestsSummary
    fi

    if [ -n "$1" ]; then
        exit 1
    fi

    exit 0
}

cleanupTestsMetadata()
{
    echo "[INFO] Running cleanup"
    aws s3 rm $S3_TESTS_SUMMARY_URL
    aws s3 rm --recursive $S3_TEST_NODES_DISCOVERY_URL
    aws s3 rm --recursive $S3_TESTS_RUNNING_URL
    aws s3 rm --recursive $S3_TESTS_WAITING_URL
    aws s3 rm --recursive $S3_TESTS_SUCCESS_URL
    aws s3 rm --recursive $S3_TESTS_FAILURE_URL
    echo "[INFO] Cleanup completed"
}

registerTestNode()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname ${S3_TEST_NODES_DISCOVERY_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to create test node registration flag: ${S3_TEST_NODES_DISCOVERY_URL}${HOST_NAME}"
    fi
}

createRunningFlag()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname ${S3_TESTS_RUNNING_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to create tests running flag: ${S3_TESTS_RUNNING_URL}${HOST_NAME}"
    fi
}

dropRunningFlag()
{
    exists=$(aws s3 ls ${S3_TESTS_RUNNING_URL}${HOST_NAME})
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm ${S3_TESTS_RUNNING_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to drop tests running flag: ${S3_TESTS_RUNNING_URL}${HOST_NAME}"
    fi
}

createWaitingFlag()
{
    aws s3 cp --sse AES256 /opt/ignite-cassandra-tests/hostname ${S3_TESTS_WAITING_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to create tests waiting flag: ${S3_TESTS_WAITING_URL}${HOST_NAME}"
    fi
}

dropWaitingFlag()
{
    exists=$(aws s3 ls ${S3_TESTS_WAITING_URL}${HOST_NAME})
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm ${S3_TESTS_WAITING_URL}${HOST_NAME}
    if [ $? -ne 0 ]; then
        terminate "Failed to drop tests waiting flag: ${S3_TESTS_WAITING_URL}${HOST_NAME}"
    fi
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

reportTestsSummary()
{
    echo "[INFO] Preparing tests results summary"

    TESTS_SUMMARY_DIR=/opt/ignite-cassandra-tests/tests-summary
    SUCCEED_NODES_FILE=$TESTS_SUMMARY_DIR/succeed-nodes
    SUCCEED_NODES_DIR=$TESTS_SUMMARY_DIR/succeed
    FAILED_NODES_FILE=$TESTS_SUMMARY_DIR/failed-nodes
    FAILED_NODES_DIR=$TESTS_SUMMARY_DIR/failed
    REPORT_FILE=$TESTS_SUMMARY_DIR/report

    rm -Rf $TESTS_SUMMARY_DIR
    mkdir -p $TESTS_SUMMARY_DIR
    mkdir -p $SUCCEED_NODES_DIR
    mkdir -p $FAILED_NODES_DIR

    aws s3 ls $S3_TESTS_SUCCESS_URL > $SUCCEED_NODES_FILE
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to get succeed nodes"
        return 1
    fi

    aws s3 ls $S3_TESTS_FAILURE_URL > $FAILED_NODES_FILE
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to get failed nodes"
        return 1
    fi

    succeedCount=$(cat $SUCCEED_NODES_FILE | wc -l)
    failedCount=$(cat $FAILED_NODES_FILE | wc -l)
    count=$(( $successCount+$failureCount ))

    echo "Test type         : $TESTS_TYPE" > $REPORT_FILE
    echo "Test nodes count  : $count" >> $REPORT_FILE
    echo "Test nodes succeed: $successCount" >> $REPORT_FILE
    echo "Test nodes failed : $failureCount" >> $REPORT_FILE
    echo "-----------------------------------------------" >> $REPORT_FILE

    if [ $failedCount -gt 0 ]; then
        echo "Failed test nodes |" >> $REPORT_FILE
        echo "------------------" >> $REPORT_FILE
        cat $FAILED_NODES_FILE >> $REPORT_FILE
        echo "-----------------------------------------------" >> $REPORT_FILE

        aws sync --delete $S3_TESTS_FAILURE_URL $FAILED_NODES_DIR
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to get failed tests details"
            return 1
        fi
    fi

    if [ $succeedCount -gt 0 ]; then
        echo "Succeed test nodes |" >> $REPORT_FILE
        echo "-------------------" >> $REPORT_FILE
        cat $SUCCEED_NODES_FILE >> $REPORT_FILE
        echo "-----------------------------------------------" >> $REPORT_FILE

        aws sync --delete $S3_TESTS_SUCCESS_URL $SUCCEED_NODES_DIR
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to get succeed tests details"
            return 1
        fi

        reportSucceedTestsStatistics "$REPORT_FILE" "$SUCCEED_NODES_DIR"
    fi

    if [ $failedCount -gt 0 ]; then
        reportFailedTestsDetailes "$REPORT_FILE" "$SUCCEED_NODES_DIR"
    fi

    aws s3 cp --sse AES256 $REPORT_FILE $S3_TESTS_SUMMARY_URL
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to report tests summary to: $S3_TESTS_SUMMARY_URL"
    fi

    rm -Rf $TESTS_SUMMARY_DIR
}

reportSucceedTestsStatistics()
{
    writeMsg=0
    writeSpeed=0
    writeErrors=0
    blkWriteMsg=0
    blkWriteSpeed=0
    blkWriteErrors=0
    readMsg=0
    readSpeed=0
    readErrors=0
    blkReadMsg=0
    blkReadSpeed=0
    blkReadErrors=0

    for dir in $2/*
    do
        node=$(echo $dir | sed -r "s/^.*\///g")
        logFile=$(ls $dir | grep "load-tests.log" | head -1)
        if [ -z "$logFile" ]; then
            echo "-----------------------------------------------" >> $1
            echo "WARNING |" >> $1
            echo "--------" >> $1
            echo "Node $node marked as succeeded, but it doesn't" >> $1
            echo "have any tests results summary file" >> $1
            continue
        fi

        cnt=$(cat $logFile | grep "WRITE messages" | sed -r "s/WRITE messages: //g")
        if [ -n "$cnt" ]; then
            writeMsg=$(( $writeMsg+$cnt ))
        fi

        cnt=$(cat $logFile | grep "WRITE errors" | sed -r "s/WRITE errors: //g" | sed -r "s/,.*//g")
        if [ -n "$cnt" ]; then
            writeSpeed=$(( $writeSpeed+$cnt ))
        fi

        cnt=$(cat $logFile | grep "WRITE speed" | sed -r "s/WRITE speed: //g" | sed -r "s/ msg\/sec//g")
        if [ -n "$cnt" ]; then
            writeErrors=$(( $writeErrors+$cnt ))
        else
            echo "-----------------------------------------------" >> $1
            echo "WARNING |" >> $1
            echo "--------" >> $1
            echo "WRITE test failed for $node node" >> $1
        fi

        cnt=$(cat $logFile | grep "BULK_WRITE messages" | sed -r "s/BULK_WRITE messages: //g")
        if [ -n "$cnt" ]; then
            blkWriteMsg=$(( $blkWriteMsg+$cnt ))
        fi

        cnt=$(cat $logFile | grep "BULK_WRITE errors" | sed -r "s/BULK_WRITE errors: //g" | sed -r "s/,.*//g")
        if [ -n "$cnt" ]; then
            blkWriteSpeed=$(( $blkWriteSpeed+$cnt ))
        fi

        cnt=$(cat $logFile | grep "BULK_WRITE speed" | sed -r "s/BULK_WRITE speed: //g" | sed -r "s/ msg\/sec//g")
        if [ -n "$cnt" ]; then
            blkWriteErrors=$(( $blkWriteErrors+$cnt ))
        else
            echo "-----------------------------------------------" >> $1
            echo "WARNING |" >> $1
            echo "--------" >> $1
            echo "BULK_WRITE test failed for $node node" >> $1
        fi

        cnt=$(cat $logFile | grep "READ messages" | sed -r "s/READ messages: //g")
        if [ -n "$cnt" ]; then
            readMsg=$(( $readMsg+$cnt ))
        fi

        cnt=$(cat $logFile | grep "READ errors" | sed -r "s/READ errors: //g" | sed -r "s/,.*//g")
        if [ -n "$cnt" ]; then
            readSpeed=$(( $readSpeed+$cnt ))
        fi

        cnt=$(cat $logFile | grep "READ speed" | sed -r "s/READ speed: //g" | sed -r "s/ msg\/sec//g")
        if [ -n "$cnt" ]; then
            readErrors=$(( $readErrors+$cnt ))
        else
            echo "-----------------------------------------------" >> $1
            echo "WARNING |" >> $1
            echo "--------" >> $1
            echo "READ test failed for $node node" >> $1
        fi

        cnt=$(cat $logFile | grep "BULK_READ messages" | sed -r "s/BULK_READ messages: //g")
        if [ -n "$cnt" ]; then
            blkReadMsg=$(( $blkReadMsg+$cnt ))
        fi

        cnt=$(cat $logFile | grep "BULK_READ errors" | sed -r "s/BULK_READ errors: //g" | sed -r "s/,.*//g")
        if [ -n "$cnt" ]; then
            blkReadSpeed=$(( $blkReadSpeed+$cnt ))
        fi

        cnt=$(cat $logFile | grep "BULK_READ speed" | sed -r "s/BULK_READ speed: //g" | sed -r "s/ msg\/sec//g")
        if [ -n "$cnt" ]; then
            blkReadErrors=$(( $blkReadErrors+$cnt ))
        else
            echo "-----------------------------------------------" >> $1
            echo "WARNING |" >> $1
            echo "--------" >> $1
            echo "BULK_READ test failed for $node node" >> $1
        fi
    done

    echo "-----------------------------------------------" >> $1
    echo "Summary WRITE test metrics |" >> $1
    echo "---------------------------" >> $1
    echo "Messages: $writeMsg" >> $1
    echo "Speed   : $writeSpeed" >> $1
    echo "Errors  : $writeErrors" >> $1

    echo "-----------------------------------------------" >> $1
    echo "Summary BULK_WRITE test metrics |" >> $1
    echo "--------------------------------" >> $1
    echo "Messages: $blkWriteMsg" >> $1
    echo "Speed   : $blkWriteSpeed" >> $1
    echo "Errors  : $blkWriteErrors" >> $1

    echo "-----------------------------------------------" >> $1
    echo "Summary READ test metrics |" >> $1
    echo "--------------------------" >> $1
    echo "Messages: $readMsg" >> $1
    echo "Speed   : $readSpeed" >> $1
    echo "Errors  : $readErrors" >> $1

    echo "-----------------------------------------------" >> $1
    echo "Summary BULK_READ test metrics |" >> $1
    echo "-------------------------------" >> $1
    echo "Messages: $blkReadMsg" >> $1
    echo "Speed   : $blkReadSpeed" >> $1
    echo "Errors  : $blkReadErrors" >> $1
}

reportFailedTestsDetailes()
{
    for dir in $2/*
    do
        node=$(echo $dir | sed -r "s/^.*\///g")
        if [ -z "$node" ]; then
            continue
        fi

        echo "-----------------------------------------------" >> $1
        echo "Error details for node: $node" >> $1
        echo "-----------------------------------------------" >> $1

        if [ -f "$dir/__error__" ]; then
            cat $dir/__error__ >> $1
        else
            echo "N/A" >> $1
        fi
    done
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

    if [ -z "$S3_TEST_NODES_DISCOVERY_URL" ]; then
        terminate "Tests S3 discovery URL doesn't specified"
    fi

    if [[ "$S3_TEST_NODES_DISCOVERY_URL" != */ ]]; then
        S3_TEST_NODES_DISCOVERY_URL=${S3_TEST_NODES_DISCOVERY_URL}/
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
}

setupCassandraSeeds()
{
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
    exists=$(aws s3 ls $S3_TESTS_FIRST_NODE_LOCK_URL)
    if [ -z "$exists" ]; then
        return 0
    fi

    aws s3 rm $S3_TESTS_FIRST_NODE_LOCK_URL
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to remove first node lock"
        return 1
    fi

    echo "[INFO] Removed first node lock"
}

waitAllIgniteNodesReady()
{
    echo "[INFO] Waiting for all $IGNITE_NODES_COUNT Ignite nodes up and running"

    while true; do
        successCount=$(aws s3 ls $S3_IGNITE_SUCCESS_URL | wc -l)
        failureCount=$(aws s3 ls $S3_IGNITE_FAILURE_URL | wc -l)

        if [ "$successCount" == "$IGNITE_NODES_COUNT" ]; then
            break
        fi

        if [ "$failureCount" != "0" ]; then
            terminate "$failureCount Ignite nodes are failed to start. Thus it doesn't make sense to run tests."
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] Congratulation, all $IGNITE_NODES_COUNT Ignite nodes are up and running"
}

waitAllCassandraNodesReady()
{
    echo "[INFO] Waiting for all $CASSANDRA_NODES_COUNT Cassandra nodes up and running"

    while true; do
        successCount=$(aws s3 ls $S3_CASSANDRA_SUCCESS_URL | wc -l)
        failureCount=$(aws s3 ls $S3_CASSANDRA_FAILURE_URL | wc -l)

        if [ "$successCount" == "$CASSANDRA_NODES_COUNT" ]; then
            break
        fi

        if [ "$failureCount" != "0" ]; then
            terminate "$failureCount Cassandra nodes are failed to start. Thus it doesn't make sense to run tests."
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] Congratulation, all $CASSANDRA_NODES_COUNT Cassandra nodes are up and running"
}

waitFirstTestNodeRegistered()
{
    echo "[INFO] Waiting for the first test node to register"

    while true; do
        first_host=

        exists=$(aws s3 ls $S3_TESTS_FIRST_NODE_LOCK_URL)
        if [ -n "$exists" ]; then
            rm -Rf /opt/ignite-cassandra-tests/first-node-lock

            aws s3 cp $S3_TESTS_FIRST_NODE_LOCK_URL /opt/ignite-cassandra-tests/first-node-lock
            if [ $? -ne 0 ]; then
                terminate "Failed to check existing first node lock"
            fi

            first_host=$(cat /opt/ignite-cassandra-tests/first-node-lock)
        fi

        if [ -n "$first_host" ]; then
            exists=$(aws s3 ls ${S3_TEST_NODES_DISCOVERY_URL}${first_host})
            if [ -n "$exists" ]; then
                break
            fi
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] First test node registered"
}

waitAllTestNodesReady()
{
    createWaitingFlag

    echo "[INFO] Waiting for all $TEST_NODES_COUNT test nodes up and running"

    while true; do

        nodesCount=$(aws s3 ls $S3_TEST_NODES_DISCOVERY_URL | wc -l)

        if [ "$nodesCount" == "$TEST_NODES_COUNT" ]; then
            break
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] Congratulation, all $TEST_NODES_COUNT test nodes are up and running"

    dropWaitingFlag
    createRunningFlag
}

waitAllTestNodesCompleted()
{
    echo "[INFO] Waiting for all $TEST_NODES_COUNT test nodes to complete their tests"

    while true; do
        successCount=$(aws s3 ls $S3_TESTS_SUCCESS_URL | wc -l)
        failureCount=$(aws s3 ls $S3_TESTS_FAILURE_URL | wc -l)
        count=$(( $successCount+$failureCount ))

        if [ "$count" == "$TEST_NODES_COUNT" ]; then
            break
        fi

        echo "[INFO] Waiting extra 1min"

        sleep 1m
    done

    echo "[INFO] Congratulation, all $TEST_NODES_COUNT test nodes have completed their tests"
}

# Time (in minutes) to wait for Ignite/Cassandra node up and running and register it in S3
NODE_STARTUP_TIME=10

HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
echo $HOST_NAME > /opt/ignite-cassandra-tests/hostname

validate

FIRST_NODE="false"

tryToGetFirstNodeLock

if [ $? -eq 0 ]; then
    FIRST_NODE="true"
fi

dropRunningFlag
dropWaitingFlag

echo "[INFO]-----------------------------------------------------------------"

if [ "$FIRST_NODE" == "true" ]; then
    echo "[INFO] Running tests from first node"
    dropTestsSummary
else
    echo "[INFO] Running tests"
fi

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Tests type: $TESTS_TYPE"
echo "[INFO] Test nodes count: $TEST_NODES_COUNT"
echo "[INFO] Ignite nodes count: $IGNITE_NODES_COUNT"
echo "[INFO] Cassandra nodes count: $CASSANDRA_NODES_COUNT"
echo "[INFO] Tests summary URL: $S3_TESTS_SUMMARY_URL"
echo "[INFO] Tests first node lock URL: $S3_TESTS_FIRST_NODE_LOCK_URL"
echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
echo "[INFO] Test node discovery URL: $S3_TEST_NODES_DISCOVERY_URL"
echo "[INFO] Ignite node discovery URL: $S3_IGNITE_NODES_DISCOVERY_URL"
echo "[INFO] Cassandra node discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
echo "[INFO] Tests running URL: $S3_TESTS_RUNNING_URL"
echo "[INFO] Tests waiting URL: $S3_TESTS_WAITING_URL"
echo "[INFO] Tests success URL: $S3_TESTS_SUCCESS_URL"
echo "[INFO] Tests failure URL: $S3_TESTS_FAILURE_URL"
echo "[INFO] Ignite success URL: $S3_IGNITE_SUCCESS_URL"
echo "[INFO] Ignite failure URL: $S3_IGNITE_FAILURE_URL"
echo "[INFO] Cassandra success URL: $S3_CASSANDRA_SUCCESS_URL"
echo "[INFO] Cassandra failure URL: $S3_CASSANDRA_FAILURE_URL"
echo "[INFO] JAVA_HOME: $JAVA_HOME"
echo "[INFO] PATH: $PATH"
echo "[INFO]-----------------------------------------------------------------"

echo "admin.user=cassandra" > /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
echo "admin.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
echo "regular.user=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties
echo "regular.password=cassandra" >> /opt/ignite-cassandra-tests/settings/org/apache/ignite/tests/cassandra/credentials.properties

waitAllCassandraNodesReady
waitAllIgniteNodesReady

setupCassandraSeeds
setupIgniteSeeds

if [ "$FIRST_NODE" != "true" ]; then
    waitFirstTestNodeRegistered
else
    cleanupTestsMetadata
fi

registerTestNode

waitAllTestNodesReady

cd /opt/ignite-cassandra-tests

if [ "$TESTS_TYPE" == "ignite" ]; then
    echo "[INFO] Running Ignite load tests"
    ./ignite-load-tests.sh
    result=$?
else
    echo "[INFO] Running Cassandra load tests"
    ./cassandra-load-tests.sh
    result=$?
fi

if [ $result -ne 0 ]; then
    terminate ""
fi

terminate
