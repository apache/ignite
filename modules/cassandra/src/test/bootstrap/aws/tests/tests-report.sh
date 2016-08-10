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
# Tests report builder
# -----------------------------------------------------------------------------------------------
# Script is used to analyze load tests logs collected from all 'Tests' cluster nodes and build
# summary report
# -----------------------------------------------------------------------------------------------

#profile=/home/ignite/.bash_profile
profile=/root/.bash_profile

. $profile
. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "test"

# Building tests summary report
reportTestsSummary()
{
    echo "[INFO] Preparing tests results summary"

    TESTS_SUMMARY_DIR=/opt/ignite-cassandra-tests/tests-summary
    SUCCEED_NODES_FILE=$TESTS_SUMMARY_DIR/succeed-nodes
    SUCCEED_NODES_DIR=$TESTS_SUMMARY_DIR/succeed
    FAILED_NODES_FILE=$TESTS_SUMMARY_DIR/failed-nodes
    FAILED_NODES_DIR=$TESTS_SUMMARY_DIR/failed
    REPORT_FILE=$TESTS_SUMMARY_DIR/report.txt

    rm -Rf $TESTS_SUMMARY_DIR
    mkdir -p $TESTS_SUMMARY_DIR
    mkdir -p $SUCCEED_NODES_DIR
    mkdir -p $FAILED_NODES_DIR

    aws s3 ls $S3_TESTS_SUCCESS | sed -r "s/PRE //g" | sed -r "s/ //g" | sed -r "s/\///g" > $SUCCEED_NODES_FILE
    aws s3 ls $S3_TESTS_FAILURE | sed -r "s/PRE //g" | sed -r "s/ //g" | sed -r "s/\///g" > $FAILED_NODES_FILE

    succeedCount=$(cat $SUCCEED_NODES_FILE | wc -l)
    failedCount=$(cat $FAILED_NODES_FILE | wc -l)
    count=$(( $succeedCount+$failedCount ))

    echo "Test type         : $TESTS_TYPE" > $REPORT_FILE
    echo "Test nodes count  : $count" >> $REPORT_FILE
    echo "Test nodes succeed: $succeedCount" >> $REPORT_FILE
    echo "Test nodes failed : $failedCount" >> $REPORT_FILE
    echo "----------------------------------------------------------------------------------------------" >> $REPORT_FILE

    if [ $succeedCount -gt 0 ]; then
        echo "Succeed test nodes |" >> $REPORT_FILE
        echo "-------------------" >> $REPORT_FILE
        cat $SUCCEED_NODES_FILE >> $REPORT_FILE
        echo "----------------------------------------------------------------------------------------------" >> $REPORT_FILE

        aws s3 sync --delete $S3_TESTS_SUCCESS $SUCCEED_NODES_DIR
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to get succeed tests details"
        else
            reportSucceedTestsStatistics "$REPORT_FILE" "$SUCCEED_NODES_DIR"
        fi
    fi

    if [ $failedCount -gt 0 ]; then
        echo "Failed test nodes |" >> $REPORT_FILE
        echo "------------------" >> $REPORT_FILE
        cat $FAILED_NODES_FILE >> $REPORT_FILE
        echo "----------------------------------------------------------------------------------------------" >> $REPORT_FILE

        aws sync --delete $S3_TESTS_FAILURE $FAILED_NODES_DIR
        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to get failed tests details"
        else
            reportFailedTestsDetailes "$REPORT_FILE" "$FAILED_NODES_DIR"
        fi
    fi

    rm -f $HOME/tests-summary.zip

    pushd $TESTS_SUMMARY_DIR

    zip -r -9 $HOME/tests-summary.zip .
    code=$?

    rm -Rf $TESTS_SUMMARY_DIR

    popd

    if [ $code -ne 0 ]; then
        echo "-------------------------------------------------------------------------------------"
        echo "[ERROR] Failed to create tests summary zip archive $HOME/tests-summary.zip for $TESTS_SUMMARY_DIR"
        echo "-------------------------------------------------------------------------------------"
        return 1
    fi

    aws s3 cp --sse AES256 $HOME/tests-summary.zip $S3_TESTS_SUMMARY
    if [ $? -ne 0 ]; then
        echo "-------------------------------------------------------------------------------------"
        echo "[ERROR] Failed to uploat tests summary archive to: $S3_TESTS_SUMMARY"
        echo "-------------------------------------------------------------------------------------"
    else
        echo "-------------------------------------------------------------------------------------"
        echo "[INFO] Tests results summary uploaded to: $S3_TESTS_SUMMARY"
        echo "-------------------------------------------------------------------------------------"
    fi

    rm -f $HOME/tests-summary.zip
}

# Creates report for succeed tests
reportSucceedTestsStatistics()
{
    writeMsg="0"
    writeErrors="0"
    writeSpeed="0"
    blkWriteMsg="0"
    blkWriteErrors="0"
    blkWriteSpeed="0"
    readMsg="0"
    readErrors="0"
    readSpeed="0"
    blkReadMsg="0"
    blkReadErrors="0"
    blkReadSpeed="0"

    writeErrNodes=
    blkWriteErrNodes=
    readErrNodes=
    blkReadErrNodes=

	tmpFile=`mktemp`

    for dir in $2/*
    do
        node=$(echo $dir | sed -r "s/^.*\///g")
        echo "-------------------------------------------------------------------------------------"
        echo "[INFO] Gathering statistics from $node test node"
        echo "-------------------------------------------------------------------------------------"

        logFile=$(ls $dir | grep "${TESTS_TYPE}-load-tests.log" | head -1)
        if [ -z "$logFile" ]; then
            echo "[WARN] Node $node marked as succeeded, but it doesn't have \"${TESTS_TYPE}-load-tests.log\" tests results summary file"
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Node $node marked as succeeded," >> $tmpFile
            echo "but it doesn't have \"${TESTS_TYPE}-load-tests.log\" tests results summary file" >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            continue
        fi

        logFile=$dir/$logFile
        if [ ! -f "$logFile" ]; then
            echo "[WARN] Node $node marked as succeeded, but it doesn't have \"${TESTS_TYPE}-load-tests.log\" tests results summary file"
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Node $node marked as succeeded," >> $tmpFile
            echo "but it doesn't have \"${TESTS_TYPE}-load-tests.log\" tests results summary file" >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            continue
        fi

        cnt=$(cat $logFile | grep "^WRITE messages" | sed -r "s/WRITE messages: //g" | xargs)
        if [ -n "$cnt" ]; then
            writeMsg=$(bc <<< "$writeMsg + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] WRITE messages: $cnt"
            else
                echo "[WARN] WRITE messages count is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "WRITE messages count is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect WRITE messages count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect WRITE messages count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^WRITE errors" | sed -r "s/WRITE errors: //g" | sed -r "s/,.*//g" | xargs)
        if [ -n "$cnt" ]; then
            echo "[INFO] WRITE errors: $cnt"
            writeErrors=$(bc <<< "$writeErrors + $cnt")
            if [ $cnt -ne 0 ]; then
                if [ -n "$writeErrNodes" ]; then
                    writeErrNodes="${writeErrNodes}, "
                fi
                writeErrNodes="${writeErrNodes}${node}"
            fi
        else
            echo "[WARN] Failed to detect WRITE errors count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect WRITE errors count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^WRITE speed" | sed -r "s/WRITE speed: //g" | sed -r "s/ msg\/sec//g" | xargs)
        if [ -n "$cnt" ]; then
            writeSpeed=$(bc <<< "$writeSpeed + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] WRITE speed: $cnt msg/sec"
            else
                echo "[WARN] WRITE speed is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "WRITE speed is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect WRITE speed for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect WRITE speed for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_WRITE messages" | sed -r "s/BULK_WRITE messages: //g" | xargs)
        if [ -n "$cnt" ]; then
            blkWriteMsg=$(bc <<< "$blkWriteMsg + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] BULK_WRITE messages: $cnt"
            else
                echo "[WARN] BULK_WRITE messages count is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "BULK_WRITE messages count is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect BULK_WRITE messages count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_WRITE messages count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_WRITE errors" | sed -r "s/BULK_WRITE errors: //g" | sed -r "s/,.*//g" | xargs)
        if [ -n "$cnt" ]; then
            blkWriteErrors=$(bc <<< "$blkWriteErrors + $cnt")
            echo "[INFO] BULK_WRITE errors: $cnt"
            if [ $cnt -ne 0 ]; then
                if [ -n "$blkWriteErrNodes" ]; then
                    blkWriteErrNodes="${blkWriteErrNodes}, "
                fi
                blkWriteErrNodes="${blkWriteErrNodes}${node}"
            fi
        else
            echo "[WARN] Failed to detect BULK_WRITE errors count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_WRITE errors count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_WRITE speed" | sed -r "s/BULK_WRITE speed: //g" | sed -r "s/ msg\/sec//g" | xargs)
        if [ -n "$cnt" ]; then
            blkWriteSpeed=$(bc <<< "$blkWriteSpeed + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] BULK_WRITE speed: $cnt msg/sec"
            else
                echo "[WARN] BULK_WRITE speed is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "BULK_WRITE speed is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect BULK_WRITE speed for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_WRITE speed for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^READ messages" | sed -r "s/READ messages: //g" | xargs)
        if [ -n "$cnt" ]; then
            readMsg=$(bc <<< "$readMsg + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] READ messages: $cnt"
            else
                echo "[WARN] READ messages count is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "READ messages count is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect READ messages count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect READ messages count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^READ errors" | sed -r "s/READ errors: //g" | sed -r "s/,.*//g" | xargs)
        if [ -n "$cnt" ]; then
            readErrors=$(bc <<< "$readErrors + $cnt")
            echo "[INFO] READ errors: $cnt"
            if [ $cnt -ne 0 ]; then
                if [ -n "$readErrNodes" ]; then
                    blkWriteErrNodes="${readErrNodes}, "
                fi
                readErrNodes="${readErrNodes}${node}"
            fi
        else
            echo "[WARN] Failed to detect READ errors count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect READ errors count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^READ speed" | sed -r "s/READ speed: //g" | sed -r "s/ msg\/sec//g" | xargs)
        if [ -n "$cnt" ]; then
            readSpeed=$(bc <<< "$readSpeed + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] READ speed: $cnt msg/sec"
            else
                echo "[WARN] READ speed is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "READ speed is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect READ speed for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect READ speed for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_READ messages" | sed -r "s/BULK_READ messages: //g" | xargs)
        if [ -n "$cnt" ]; then
            blkReadMsg=$(bc <<< "$blkReadMsg + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] BULK_READ messages: $cnt"
            else
                echo "[WARN] BULK_READ messages count is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "BULK_READ messages count is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect BULK_READ messages count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_READ messages count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_READ errors" | sed -r "s/BULK_READ errors: //g" | sed -r "s/,.*//g" | xargs)
        if [ -n "$cnt" ]; then
            blkReadErrors=$(bc <<< "$blkReadErrors + $cnt")
            echo "[INFO] BULK_READ errors: $cnt"
            if [ $cnt -ne 0 ]; then
                if [ -n "$blkReadErrNodes" ]; then
                    blkReadErrNodes="${blkReadErrNodes}, "
                fi
                blkReadErrNodes="${blkReadErrNodes}${node}"
            fi
        else
            echo "[WARN] Failed to detect BULK_READ errors count for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_READ errors count for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi

        cnt=$(cat $logFile | grep "^BULK_READ speed" | sed -r "s/BULK_READ speed: //g" | sed -r "s/ msg\/sec//g" | xargs)
        if [ -n "$cnt" ]; then
            blkReadSpeed=$(bc <<< "$blkReadSpeed + $cnt")
            if [ $cnt -ne 0 ]; then
                echo "[INFO] BULK_READ speed: $cnt msg/sec"
            else
                echo "[WARN] BULK_READ speed is zero for $node node. This test probably failed."
                echo "WARNING |" >> $tmpFile
                echo "--------" >> $tmpFile
                echo "BULK_READ speed is zero for $node node. This test probably failed." >> $tmpFile
                echo "----------------------------------------------------------------------------------------------" >> $tmpFile
            fi
        else
            echo "[WARN] Failed to detect BULK_READ speed for $node node. This test probably failed."
            echo "WARNING |" >> $tmpFile
            echo "--------" >> $tmpFile
            echo "Failed to detect BULK_READ speed for $node node. This test probably failed." >> $tmpFile
            echo "----------------------------------------------------------------------------------------------" >> $tmpFile
        fi
    done

    echo "-------------------------------------------------------------------------------------"

    echo "WRITE test metrics |" >> $1
    echo "-------------------" >> $1
    echo "Messages: $writeMsg" >> $1
    echo "Speed   : $writeSpeed msg/sec" >> $1
    echo "Errors  : $writeErrors" >> $1
    echo "----------------------------------------------------------------------------------------------" >> $1

    echo "BULK_WRITE test metrics |" >> $1
    echo "------------------------" >> $1
    echo "Messages: $blkWriteMsg" >> $1
    echo "Speed   : $blkWriteSpeed msg/sec" >> $1
    echo "Errors  : $blkWriteErrors" >> $1
    echo "----------------------------------------------------------------------------------------------" >> $1

    echo "READ test metrics |" >> $1
    echo "------------------" >> $1
    echo "Messages: $readMsg" >> $1
    echo "Speed   : $readSpeed msg/sec" >> $1
    echo "Errors  : $readErrors" >> $1
    echo "----------------------------------------------------------------------------------------------" >> $1

    echo "BULK_READ test metrics |" >> $1
    echo "-----------------------" >> $1
    echo "Messages: $blkReadMsg" >> $1
    echo "Speed   : $blkReadSpeed msg/sec" >> $1
    echo "Errors  : $blkReadErrors" >> $1
    echo "----------------------------------------------------------------------------------------------" >> $1

    if [ -n "$writeErrNodes" ]; then
        echo "Nodes having WRITE errors |" >> $1
        echo "-------------------------------" >> $1
        echo "$writeErrNodes" >> $1
        echo "----------------------------------------------------------------------------------------------" >> $1
    fi

    if [ -n "$blkWriteErrNodes" ]; then
        echo "Nodes having BULK_WRITE errors |" >> $1
        echo "-------------------------------" >> $1
        echo "$blkWriteErrNodes" >> $1
        echo "----------------------------------------------------------------------------------------------" >> $1
    fi

    if [ -n "$readErrNodes" ]; then
        echo "Nodes having READ errors |" >> $1
        echo "-------------------------------" >> $1
        echo "$readErrNodes" >> $1
        echo "----------------------------------------------------------------------------------------------" >> $1
    fi

    if [ -n "$blkReadErrNodes" ]; then
        echo "Nodes having BULK_READ errors |" >> $1
        echo "-------------------------------" >> $1
        echo "$blkReadErrNodes" >> $1
        echo "----------------------------------------------------------------------------------------------" >> $1
    fi

    cat $tmpFile >> $1

    rm -f $tmpFile
}

# Creates report for failed tests
reportFailedTestsDetailes()
{
    for dir in $2/*
    do
        node=$(echo $dir | sed -r "s/^.*\///g")
        if [ -z "$node" ]; then
            continue
        fi

        echo "----------------------------------------------------------------------------------------------" >> $1
        echo "Error details for node: $node" >> $1
        echo "----------------------------------------------------------------------------------------------" >> $1

        if [ -f "$dir/__error__" ]; then
            cat $dir/__error__ >> $1
        else
            echo "N/A" >> $1
        fi
    done
}

#######################################################################################################

if [ "$TESTS_TYPE" != "ignite" ] && [ "$TESTS_TYPE" != "cassandra" ]; then
    terminate "Incorrect tests type specified: $TESTS_TYPE"
fi

reportTestsSummary