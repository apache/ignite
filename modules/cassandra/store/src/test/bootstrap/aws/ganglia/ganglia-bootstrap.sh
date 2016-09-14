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
# Bootstrap script to spin up Ganglia master
# -----------------------------------------------------------------------------------------------

# URL to download AWS CLI tools
AWS_CLI_DOWNLOAD_URL=https://s3.amazonaws.com/aws-cli/awscli-bundle.zip

# URL to download JDK
JDK_DOWNLOAD_URL=http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.tar.gz

# URL to download Ignite-Cassandra tests package - you should previously package and upload it to this place
TESTS_PACKAGE_DONLOAD_URL=s3://<bucket>/<folder>/ignite-cassandra-tests-<version>.zip

# Terminates script execution and upload logs to S3
terminate()
{
    SUCCESS_URL=$S3_GANGLIA_BOOTSTRAP_SUCCESS
    FAILURE_URL=$S3_GANGLIA_BOOTSTRAP_FAILURE

    if [ -n "$SUCCESS_URL" ] && [[ "$SUCCESS_URL" != */ ]]; then
        SUCCESS_URL=${SUCCESS_URL}/
    fi

    if [ -n "$FAILURE_URL" ] && [[ "$FAILURE_URL" != */ ]]; then
        FAILURE_URL=${FAILURE_URL}/
    fi

    host_name=$(hostname -f | tr '[:upper:]' '[:lower:]')
    msg=$host_name

    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Ganglia master node bootstrap failed"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1

        if [ -z "$FAILURE_URL" ]; then
            exit 1
        fi

        reportFolder=${FAILURE_URL}${host_name}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Ganglia master node bootstrap successfully completed"
        echo "[INFO]-----------------------------------------------------"

        if [ -z "$SUCCESS_URL" ]; then
            exit 0
        fi

        reportFolder=${SUCCESS_URL}${host_name}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/bootstrap-result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to drop report folder: $reportFolder"
    fi

    aws s3 cp --sse AES256 /opt/bootstrap-result $reportFile
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to report bootstrap result to: $reportFile"
    fi

    rm -f /opt/bootstrap-result

    if [ -n "$1" ]; then
        exit 1
    fi

    exit 0
}

# Downloads specified package
downloadPackage()
{
    echo "[INFO] Downloading $3 package from $1 into $2"

    for i in 0 9;
    do
        if [[ "$1" == s3* ]]; then
            aws s3 cp $1 $2
            code=$?
        else
            curl "$1" -o "$2"
            code=$?
        fi

        if [ $code -eq 0 ]; then
            echo "[INFO] $3 package successfully downloaded from $1 into $2"
            return 0
        fi

        echo "[WARN] Failed to download $3 package from $i attempt, sleeping extra 5sec"
        sleep 5s
    done

    terminate "All 10 attempts to download $3 package from $1 are failed"
}

# Downloads and setup JDK
setupJava()
{
    rm -Rf /opt/java /opt/jdk.tar.gz

    echo "[INFO] Downloading 'jdk'"
    wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "$JDK_DOWNLOAD_URL" -O /opt/jdk.tar.gz
    if [ $? -ne 0 ]; then
        terminate "Failed to download 'jdk'"
    fi

    echo "[INFO] Untaring 'jdk'"
    tar -xvzf /opt/jdk.tar.gz -C /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to untar 'jdk'"
    fi

    rm -Rf /opt/jdk.tar.gz

    unzipDir=$(ls /opt | grep "jdk")
    if [ "$unzipDir" != "java" ]; then
        mv /opt/$unzipDir /opt/java
    fi
}

# Downloads and setup AWS CLI
setupAWSCLI()
{
    echo "[INFO] Installing 'awscli'"
    pip install --upgrade awscli
    if [ $? -eq 0 ]; then
        return 0
    fi

    echo "[ERROR] Failed to install 'awscli' using pip"
    echo "[INFO] Trying to install awscli using zip archive"
    echo "[INFO] Downloading awscli zip"

    downloadPackage "$AWS_CLI_DOWNLOAD_URL" "/opt/awscli-bundle.zip" "awscli"

    echo "[INFO] Unzipping awscli zip"
    unzip /opt/awscli-bundle.zip -d /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to unzip awscli zip"
    fi

    rm -Rf /opt/awscli-bundle.zip

    echo "[INFO] Installing awscli"
    /opt/awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
    if [ $? -ne 0 ]; then
        terminate "Failed to install awscli"
    fi

    echo "[INFO] Successfully installed awscli from zip archive"
}

# Setup all the pre-requisites (packages, settings and etc.)
setupPreRequisites()
{
    echo "[INFO] Installing 'wget' package"
    yum -y install wget
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'wget' package"
    fi

    echo "[INFO] Installing 'net-tools' package"
    yum -y install net-tools
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'net-tools' package"
    fi

    echo "[INFO] Installing 'python' package"
    yum -y install python
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'python' package"
    fi

    echo "[INFO] Installing 'unzip' package"
    yum -y install unzip
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'unzip' package"
    fi

    downloadPackage "https://bootstrap.pypa.io/get-pip.py" "/opt/get-pip.py" "get-pip.py"

    echo "[INFO] Installing 'pip'"
    python /opt/get-pip.py
    if [ $? -ne 0 ]; then
        terminate "Failed to install 'pip'"
    fi
}

# Downloads and setup tests package
setupTestsPackage()
{
    downloadPackage "$TESTS_PACKAGE_DONLOAD_URL" "/opt/ignite-cassandra-tests.zip" "Tests"

    rm -Rf /opt/ignite-cassandra-tests

    unzip /opt/ignite-cassandra-tests.zip -d /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to unzip tests package"
    fi

    rm -f /opt/ignite-cassandra-tests.zip

    unzipDir=$(ls /opt | grep "ignite-cassandra")
    if [ "$unzipDir" != "ignite-cassandra-tests" ]; then
        mv /opt/$unzipDir /opt/ignite-cassandra-tests
    fi

    find /opt/ignite-cassandra-tests -type f -name "*.sh" -exec chmod ug+x {} \;

    . /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "ganglia"

    setupNTP

    echo "[INFO] Starting logs collector daemon"

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
    /opt/ignite-cassandra-tests/bootstrap/aws/logs-collector.sh "$S3_LOGS_TRIGGER" "$S3_GANGLIA_LOGS/$HOST_NAME" "/var/log/httpd" > /opt/logs-collector.log &

    echo "[INFO] Logs collector daemon started: $!"

    echo "----------------------------------------------------------------------------------------"
    printInstanceInfo
    echo "----------------------------------------------------------------------------------------"
    tagInstance
}

# Creates config file for 'gmond' damon working in receiver mode
createGmondReceiverConfig()
{
    /usr/local/sbin/gmond --default_config > /opt/gmond-default.conf
    if [ $? -ne 0 ]; then
        terminate "Failed to create gmond default config in: /opt/gmond-default.txt"
    fi

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    cat /opt/gmond-default.conf | sed -r "s/mute = no/mute = yes/g" | \
    sed -r "s/name = \"unspecified\"/name = \"$1\"/g" | \
    sed -r "s/#bind_hostname/bind_hostname/g" | \
    sed "0,/mcast_join = 239.2.11.71/s/mcast_join = 239.2.11.71/host = $HOST_NAME/g" | \
    sed -r "s/mcast_join = 239.2.11.71//g" | sed -r "s/bind = 239.2.11.71//g" | \
    sed -r "s/port = 8649/port = $2/g" | sed -r "s/retry_bind = true//g" > /opt/gmond-${1}.conf

    chmod a+r /opt/gmond-${1}.conf

    rm -f /opt/gmond-default.conf
}

# Creates config file for 'gmond' damon working in sender-receiver mode
createGmondSenderReceiverConfig()
{
    /usr/local/sbin/gmond --default_config > /opt/gmond-default.conf
    if [ $? -ne 0 ]; then
        terminate "Failed to create gmond default config in: /opt/gmond-default.txt"
    fi

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    cat /opt/gmond-default.conf | sed -r "s/name = \"unspecified\"/name = \"$1\"/g" | \
    sed -r "s/#bind_hostname/bind_hostname/g" | \
    sed "0,/mcast_join = 239.2.11.71/s/mcast_join = 239.2.11.71/host = $HOST_NAME/g" | \
    sed -r "s/mcast_join = 239.2.11.71//g" | sed -r "s/bind = 239.2.11.71//g" | \
    sed -r "s/port = 8649/port = $2/g" | sed -r "s/retry_bind = true//g" > /opt/gmond-${1}.conf

    chmod a+r /opt/gmond-${1}.conf

    rm -f /opt/gmond-default.conf
}

# Downloads and setup Ganglia (and dependency) packages
setupGangliaPackages()
{
    installGangliaPackages "master"

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

    echo "data_source \"cassandra\" ${HOST_NAME}:8641" > /opt/gmetad.conf
    echo "data_source \"ignite\" ${HOST_NAME}:8642" >> /opt/gmetad.conf
    echo "data_source \"test\" ${HOST_NAME}:8643" >> /opt/gmetad.conf
    #echo "data_source \"ganglia\" ${HOST_NAME}:8644" >> /opt/gmetad.conf
    echo "setuid_username \"nobody\"" >> /opt/gmetad.conf
    echo "case_sensitive_hostnames 0" >> /opt/gmetad.conf

    chmod a+r /opt/gmetad.conf

    createGmondReceiverConfig cassandra 8641
    createGmondReceiverConfig ignite 8642
    createGmondReceiverConfig test 8643
    #createGmondSenderReceiverConfig ganglia 8644
}

# Starts 'gmond' receiver damon
startGmondReceiver()
{
    configFile=/opt/gmond-${1}.conf
    pidFile=/opt/gmond-${1}.pid

    echo "[INFO] Starting gmond receiver daemon for $1 cluster using config file: $configFile"

    rm -f $pidFile

    /usr/local/sbin/gmond --conf=$configFile --pid-file=$pidFile

    sleep 2s

    if [ ! -f "$pidFile" ]; then
        terminate "Failed to start gmond daemon for $1 cluster, pid file doesn't exist"
    fi

    pid=$(cat $pidFile)

    echo "[INFO] gmond daemon for $1 cluster started, pid=$pid"

    exists=$(ps $pid | grep gmond)

    if [ -z "$exists" ]; then
        terminate "gmond daemon for $1 cluster abnormally terminated"
    fi
}

# Starts 'gmetad' daemon
startGmetadCollector()
{
    echo "[INFO] Starting gmetad daemon"

    rm -f /opt/gmetad.pid

    /usr/local/sbin/gmetad --conf=/opt/gmetad.conf --pid-file=/opt/gmetad.pid

    sleep 2s

    if [ ! -f "/opt/gmetad.pid" ]; then
        terminate "Failed to start gmetad daemon, pid file doesn't exist"
    fi

    pid=$(cat /opt/gmetad.pid)

    echo "[INFO] gmetad daemon started, pid=$pid"

    exists=$(ps $pid | grep gmetad)

    if [ -z "$exists" ]; then
        terminate "gmetad daemon abnormally terminated"
    fi
}

# Starts Apache 'httpd' service
startHttpdService()
{
    echo "[INFO] Starting httpd service"

    service httpd start

    if [ $? -ne 0 ]; then
        terminate "Failed to start httpd service"
    fi

    sleep 5s

    exists=$(service httpd status | grep running)
    if [ -z "$exists" ]; then
        terminate "httpd service process terminated"
    fi

    echo "[INFO] httpd service successfully started"
}

###################################################################################################################

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Bootstrapping Ganglia master server"
echo "[INFO]-----------------------------------------------------------------"

setupPreRequisites
setupJava
setupAWSCLI
setupTestsPackage
setupGangliaPackages

registerNode

startGmondReceiver cassandra
startGmondReceiver ignite
startGmondReceiver test
#startGmondReceiver ganglia
startGmetadCollector
startHttpdService

terminate
