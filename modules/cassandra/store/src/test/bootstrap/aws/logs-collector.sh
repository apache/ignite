#!/bin/sh

#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
#

# -----------------------------------------------------------------------------------------------
# Logs collector daemon
# -----------------------------------------------------------------------------------------------
# Script is launched in background by all EC2 nodes of all clusters (Cassandra, Ignite, Tests) and
# periodically (each 30 seconds) checks if specific S3 trigger file (specified by $S3_LOGS_TRIGGER_URL)
# was created or its timestamp was changed. Such an event serve as a trigger for the script
# to collect EC2 instance logs (from folder specified by $1) and upload them into specific
# S3 folder (specified by $S3_LOGS_FOLDER).
# -----------------------------------------------------------------------------------------------

uploadLogs()
{
    if [ ! -d "$1" ]; then
        echo "[INFO] Logs directory doesn't exist: $1"
        return 0
    fi

    echo "[INFO] Uploading logs from directory: $1"

    dirList=$(ls $1 | head -1)

    if [ -z "$dirList" ]; then
        echo "[INFO] Directory is empty: $1"
    fi

    for i in 0 9;
    do
        aws s3 sync --sse AES256 --delete "$1" "$S3_LOGS_FOLDER"
        code=$?

        if [ $code -eq 0 ]; then
            echo "[INFO] Successfully uploaded logs from directory: $1"
            return 0
        fi

        echo "[WARN] Failed to upload logs from $i attempt, sleeping extra 30sec"
        sleep 30s
    done

    echo "[ERROR] All 10 attempts to upload logs are failed for the directory: $1"
}

createNewLogsSnapshot()
{
    rm -f ~/logs-collector.snapshot.new

    for log_src in "$@"
    do
        if [ -d "$log_src" ] || [ -f "$log_src" ]; then
            ls -alR $log_src >> ~/logs-collector.snapshot.new

        fi
    done
}

checkLogsChanged()
{
    createNewLogsSnapshot $@

    if [ ! -f "~/logs-collector.snapshot" ]; then
        return 1
    fi

    diff "~/logs-collector.snapshot" "~/logs-collector.snapshot.new" > /dev/null

    return $?
}

updateLogsSnapshot()
{
    if [ ! -f "~/logs-collector.snapshot.new" ]; then
        return 0
    fi

    rm -f "~/logs-collector.snapshot"
    mv "~/logs-collector.snapshot.new" "~/logs-collector.snapshot"
}

collectLogs()
{
    createNewLogsSnapshot

    rm -Rf ~/logs-collector-logs
    mkdir -p ~/logs-collector-logs

    for log_src in "$@"
    do
        if [ -f "$log_src" ]; then
            echo "[INFO] Collecting log file: $log_src"
            cp -f $log_src ~/logs-collector-logs
        elif [ -d "$log_src" ]; then
            echo "[INFO] Collecting logs from folder: $log_src"
            cp -Rf $log_src ~/logs-collector-logs
        fi
    done

    uploadLogs ~/logs-collector-logs

    rm -Rf ~/logs-collector-logs

    updateLogsSnapshot
}

echo "[INFO] Running Logs collector service"

if [ -z "$1" ]; then
    echo "[ERROR] Logs collection S3 trigger URL doesn't specified"
    exit 1
fi

S3_LOGS_TRIGGER_URL=$1

echo "[INFO] Logs collection S3 trigger URL: $S3_LOGS_TRIGGER_URL"

if [ -z "$2" ]; then
    echo "[ERROR] S3 folder where to upload logs doesn't specified"
    exit 1
fi

S3_LOGS_FOLDER=$2

echo "[INFO] S3 logs upload folder: $S3_LOGS_FOLDER"

shift 2

if [ -z "$1" ]; then
    echo "[WARN] Local logs sources don't specified"
else
    echo "[INFO] Local logs sources: $@"
fi

echo "--------------------------------------------------------------------"

TRIGGER_STATE=

while true; do
    sleep 30s

    STATE=$(aws s3 ls $S3_LOGS_TRIGGER_URL)

    if [ -z "$STATE" ] || [ "$STATE" == "$TRIGGER_STATE" ]; then
        checkLogsChanged

        if [ $? -eq 0 ]; then
            continue
        fi
    fi

    TRIGGER_STATE=$STATE

    collectLogs $@ /var/log/cloud-init.log /var/log/cloud-init-output.log

    echo "--------------------------------------------------------------------"
done
