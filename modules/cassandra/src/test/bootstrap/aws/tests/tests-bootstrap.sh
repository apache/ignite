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

AWS_CLI_DOWNLOAD_URL=https://s3.amazonaws.com/aws-cli/awscli-bundle.zip

JDK_DOWNLOAD_URL=http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.tar.gz

TESTS_PACKAGE_DONLOAD_URL=s3://bucket/folder/ignite-cassandra-tests-1.6.0-SNAPSHOT.zip

terminate()
{
    SUCCESS_URL=$S3_TESTS_SUCCESS
    FAILURE_URL=$S3_TESTS_FAILURE

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
        echo "[ERROR] Test node bootstrap failed"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1

        if [ -z "$FAILURE_URL" ]; then
            exit 1
        fi

        reportFolder=${FAILURE_URL}${host_name}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Test node bootstrap successfully completed"
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

downloadPackage()
{
    echo "[INFO] Downloading $3 package from $1 into $2"

    if [[ "$1" == s3* ]]; then
        aws s3 cp $1 $2

        if [ $? -ne 0 ]; then
            echo "[WARN] Failed to download $3 package from first attempt"
            rm -Rf $2
            sleep 10s

            echo "[INFO] Trying second attempt to download $3 package"
            aws s3 cp $1 $2

            if [ $? -ne 0 ]; then
                echo "[WARN] Failed to download $3 package from second attempt"
                rm -Rf $2
                sleep 10s

                echo "[INFO] Trying third attempt to download $3 package"
                aws s3 cp $1 $2

                if [ $? -ne 0 ]; then
                    terminate "All three attempts to download $3 package from $1 are failed"
                fi
            fi
        fi
    else
        curl "$1" -o "$2"

        if [ $? -ne 0 ] && [ $? -ne 6 ]; then
            echo "[WARN] Failed to download $3 package from first attempt"
            rm -Rf $2
            sleep 10s

            echo "[INFO] Trying second attempt to download $3 package"
            curl "$1" -o "$2"

            if [ $? -ne 0 ] && [ $? -ne 6 ]; then
                echo "[WARN] Failed to download $3 package from second attempt"
                rm -Rf $2
                sleep 10s

                echo "[INFO] Trying third attempt to download $3 package"
                curl "$1" -o "$2"

                if [ $? -ne 0 ] && [ $? -ne 6 ]; then
                    terminate "All three attempts to download $3 package from $1 are failed"
                fi
            fi
        fi
    fi

    echo "[INFO] $3 package successfully downloaded from $1 into $2"
}

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

    . /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "test"
    . /opt/ignite-cassandra-tests/bootstrap/aws/ganglia/agent-bootstrap.sh

    echo "----------------------------------------------------------------------------------------"
    printInstanceInfo
    echo "----------------------------------------------------------------------------------------"
    tagInstance
    bootstrapGangliaAgent "test"

    ###################################################
    # Extra configuration specific only for test node #
    ###################################################

    echo "[INFO] Creating 'ignite' group"
    exists=$(cat /etc/group | grep ignite)
    if [ -z "$exists" ]; then
        groupadd ignite
        if [ $? -ne 0 ]; then
            terminate "Failed to create 'ignite' group"
        fi
    fi

    echo "[INFO] Creating 'ignite' user"
    exists=$(cat /etc/passwd | grep ignite)
    if [ -z "$exists" ]; then
        useradd -g ignite ignite
        if [ $? -ne 0 ]; then
            terminate "Failed to create 'ignite' user"
        fi
    fi

    mkdir -p /opt/ignite-cassandra-tests/logs
    chown -R ignite:ignite /opt/ignite-cassandra-tests

    echo "export JAVA_HOME=/opt/jdk1.8.0_77" >> $1
    echo "export PATH=\$JAVA_HOME/bin:\IGNITE_HOME/bin:\$PATH" >> $1

    echo "[INFO] Starting logs collector daemon"

    HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')
    /opt/ignite-cassandra-tests/bootstrap/aws/logs-collector.sh "/opt/ignite-cassandra-tests/logs" "$S3_TESTS_LOGS/$HOST_NAME" "$S3_LOGS_TRIGGER" > /opt/ignite-cassandra-tests/logs-collector.log &

    echo "[INFO] Logs collector daemon started: $!"
}

###################################################################################################################

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Bootstrapping Tests node"
echo "[INFO]-----------------------------------------------------------------"

setupPreRequisites
setupJava
setupAWSCLI
setupTestsPackage "/root/.bash_profile"

cmd="/opt/ignite-cassandra-tests/bootstrap/aws/tests/tests-manager.sh"

#sudo -u ignite -g ignite sh -c "$cmd > /opt/ignite-cassandra-tests/tests-manager" &

$cmd > /opt/ignite-cassandra-tests/logs/tests-manager.log &

terminate