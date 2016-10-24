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

echo "[INFO] Running Logs collector service"

if [ -z "$1" ]; then
    echo "[ERROR] Local logs directory doesn't specified"
    exit 1
fi

echo "[INFO] Local logs directory: $1"

if [ -z "$2" ]; then
    echo "[ERROR] S3 folder where to upload logs doesn't specified"
    exit 1
fi

echo "[INFO] S3 logs upload folder: $2"

if [ -z "$3" ]; then
    echo "[ERROR] Logs collection S3 trigger URL doesn't specified"
    exit 1
fi

echo "[INFO] Logs collection S3 trigger URL: $3"

echo "--------------------------------------------------------------------"

TRIGGER_STATE=

while true; do
    sleep 1m

    STATE=$(aws s3 ls $3)

    if [ -z "$STATE" ] || [ "$STATE" == "$TRIGGER_STATE" ]; then
        continue
    fi

    TRIGGER_STATE=$STATE

    exists=
    if [ -d "$1" ]; then
        exists="true"
    fi

    echo "[INFO] Uploading logs from $1 to $2"

    if [ "$exists" != "true" ]; then
        echo "[INFO] Local logs directory $1 doesn't exist, thus there is nothing to upload"
    fi

    echo "--------------------------------------------------------------------"

    if [ "$exists" != "true" ]; then
        continue
    fi

    aws s3 sync --sse AES256 --delete "$1" "$2"

    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to upload logs from $1 to $2 from first attempt"
        sleep 30s

        aws s3 sync --sse AES256 --delete "$1" "$2"

        if [ $? -ne 0 ]; then
            echo "[ERROR] Failed to upload logs from $1 to $2 from second attempt"
            sleep 1m

            aws s3 sync --sse AES256 --delete "$1" "$2"

            if [ $? -ne 0 ]; then
                echo "[ERROR] Failed to upload logs from $1 to $2 from third attempt"
            else
                echo "[INFO] Logs successfully uploaded from $1 to $2 from third attempt"
            fi
        else
            echo "[INFO] Logs successfully uploaded from $1 to $2 from second attempt"
        fi
    else
        echo "[INFO] Logs successfully uploaded from $1 to $2"
    fi

    echo "--------------------------------------------------------------------"
done
