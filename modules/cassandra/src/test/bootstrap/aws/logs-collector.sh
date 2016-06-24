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
    if [ ! -d "$1" ] && [ ! -f "$1" ]; then
        echo "[INFO] Logs source doesn't exist: $1"
        return 0
    fi

    if [ -d "$1" ]; then
        echo "[INFO] Uploading logs from directory: $1"

        dirList=$(ls $1 | head -1)

        if [ -z "$dirList" ]; then
            echo "[INFO] Directory is empty: $1"
            return 0
        fi
    else
        echo "[INFO] Uploading logs file: $1"
    fi

    for i in 0 9;
    do
        if [ -d "$1" ]; then
            aws s3 sync --sse AES256 "$1" "$S3_LOGS_FOLDER"
            code=$?
        else
            aws s3 cp --sse AES256 "$1" "${S3_LOGS_FOLDER}/"
            code=$?
        fi

        if [ $code -eq 0 ]; then
            if [ -d "$1" ]; then
                echo "[INFO] Successfully uploaded logs from directory: $1"
            else
                echo "[INFO] Successfully uploaded logs file: $1"
            fi

            return 0
        fi

        echo "[WARN] Failed to upload logs from $i attempt, sleeping extra 30sec"
        sleep 30s
    done

    if [ -d "$1" ]; then
        echo "[ERROR] All 10 attempts to upload logs are failed for the directory: $1"
    else
        echo "[ERROR] All 10 attempts to upload logs are failed for the file: $1"
    fi
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
        continue
    fi

    TRIGGER_STATE=$STATE

    echo "[INFO] Cleaning S3 logs folder: $S3_LOGS_FOLDER"

    aws s3 rm --recursive $S3_LOGS_FOLDER

    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to clean S3 logs folder: $S3_LOGS_FOLDER"
    fi

    for log_src in "$@"
    do
        uploadLogs $log_src
    done

    uploadLogs "/var/log/cloud-init.log"
    uploadLogs "/var/log/cloud-init-output.log"

    echo "--------------------------------------------------------------------"
done
