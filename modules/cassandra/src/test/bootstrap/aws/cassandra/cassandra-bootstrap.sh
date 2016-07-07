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

AWS_CLI_DOWNLOAD_URL=https://s3.amazonaws.com/aws-cli/awscli-bundle.zip

S3_ROOT=s3://bucket/folder
S3_DOWNLOADS=$S3_ROOT/test
S3_SYSTEM=$S3_ROOT/test1

CASSANDRA_DOWNLOAD_URL=http://www-eu.apache.org/dist/cassandra/3.5/apache-cassandra-3.5-bin.tar.gz
CASSANDRA_TARBALL=apache-cassandra-3.5-bin.tar.gz
CASSANDRA_UNTAR_DIR=apache-cassandra-3.5

TESTS_PACKAGE_DONLOAD_URL=$S3_DOWNLOADS/ignite-cassandra-tests-1.6.0-SNAPSHOT.zip
TESTS_PACKAGE_ZIP=ignite-cassandra-tests-1.6.0-SNAPSHOT.zip
TESTS_PACKAGE_UNZIP_DIR=ignite-cassandra-tests

S3_LOGS_URL=$S3_SYSTEM/logs/c-logs
S3_LOGS_TRIGGER_URL=$S3_SYSTEM/logs-trigger
S3_BOOTSTRAP_SUCCESS_URL=$S3_SYSTEM/c-success
S3_BOOTSTRAP_FAILURE_URL=$S3_SYSTEM/c-failure
S3_CASSANDRA_NODES_DISCOVERY_URL=$S3_SYSTEM/c-discovery
S3_CASSANDRA_FIRST_NODE_LOCK_URL=$S3_SYSTEM/c-first-node-lock
S3_CASSANDRA_NODES_JOIN_LOCK_URL=$S3_SYSTEM/c-join-lock

INSTANCE_REGION=us-west-2
INSTANCE_NAME_TAG=CASSANDRA-SERVER
INSTANCE_OWNER_TAG=ignite@apache.org
INSTANCE_PROJECT_TAG=ignite

terminate()
{
    if [[ "$S3_BOOTSTRAP_SUCCESS_URL" != */ ]]; then
        S3_BOOTSTRAP_SUCCESS_URL=${S3_BOOTSTRAP_SUCCESS_URL}/
    fi

    if [[ "$S3_BOOTSTRAP_FAILURE_URL" != */ ]]; then
        S3_BOOTSTRAP_FAILURE_URL=${S3_BOOTSTRAP_FAILURE_URL}/
    fi

    host_name=$(hostname -f | tr '[:upper:]' '[:lower:]')
    msg=$host_name

    if [ -n "$1" ]; then
        echo "[ERROR] $1"
        echo "[ERROR]-----------------------------------------------------"
        echo "[ERROR] Cassandra node bootstrap failed"
        echo "[ERROR]-----------------------------------------------------"
        msg=$1
        reportFolder=${S3_BOOTSTRAP_FAILURE_URL}${host_name}
        reportFile=$reportFolder/__error__
    else
        echo "[INFO]-----------------------------------------------------"
        echo "[INFO] Cassandra node bootstrap successfully completed"
        echo "[INFO]-----------------------------------------------------"
        reportFolder=${S3_BOOTSTRAP_SUCCESS_URL}${host_name}
        reportFile=$reportFolder/__success__
    fi

    echo $msg > /opt/bootstrap-result

    aws s3 rm --recursive $reportFolder
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed drop report folder: $reportFolder"
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

tagInstance()
{
    export EC2_HOME=/opt/aws/apitools/ec2
    export JAVA_HOME=/opt/jdk1.8.0_77
    export PATH=$JAVA_HOME/bin:$EC2_HOME/bin:$PATH

    INSTANCE_ID=$(curl http://169.254.169.254/latest/meta-data/instance-id)
    if [ $? -ne 0 ]; then
        terminate "Failed to get instance metadata to tag it"
    fi

    if [ -n "$INSTANCE_NAME_TAG" ]; then
        ec2-create-tags $INSTANCE_ID --tag Name=${INSTANCE_NAME_TAG} --region $INSTANCE_REGION
        if [ $code -ne 0 ]; then
            terminate "Failed to tag EC2 instance with: Name=${INSTANCE_NAME_TAG}"
        fi
    fi

    if [ -n "$INSTANCE_OWNER_TAG" ]; then
        ec2-create-tags $INSTANCE_ID --tag owner=${INSTANCE_OWNER_TAG} --region $INSTANCE_REGION
        if [ $code -ne 0 ]; then
            terminate "Failed to tag EC2 instance with: owner=${INSTANCE_OWNER_TAG}"
        fi
    fi

    if [ -n "$INSTANCE_PROJECT_TAG" ]; then
        ec2-create-tags $INSTANCE_ID --tag project=${INSTANCE_PROJECT_TAG} --region $INSTANCE_REGION
        if [ $code -ne 0 ]; then
            terminate "Failed to tag EC2 instance with: project=${INSTANCE_PROJECT_TAG}"
        fi
    fi
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

if [[ "$S3_CASSANDRA_NODES_DISCOVERY_URL" != */ ]]; then
    S3_CASSANDRA_NODES_DISCOVERY_URL=${S3_CASSANDRA_NODES_DISCOVERY_URL}/
fi

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Bootstrapping Cassandra node"
echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Cassandra download URL: $CASSANDRA_DOWNLOAD_URL"
echo "[INFO] Tests package download URL: $TESTS_PACKAGE_DONLOAD_URL"
echo "[INFO] Logs URL: $S3_LOGS_URL"
echo "[INFO] Logs trigger URL: $S3_LOGS_TRIGGER_URL"
echo "[INFO] Cassandra nodes discovery URL: $S3_CASSANDRA_NODES_DISCOVERY_URL"
echo "[INFO] Cassandra first node lock URL: $S3_CASSANDRA_FIRST_NODE_LOCK_URL"
echo "[INFO] Cassandra nodes join lock URL: $S3_CASSANDRA_NODES_JOIN_LOCK_URL"
echo "[INFO] Bootsrap success URL: $S3_BOOTSTRAP_SUCCESS_URL"
echo "[INFO] Bootsrap failure URL: $S3_BOOTSTRAP_FAILURE_URL"
echo "[INFO]-----------------------------------------------------------------"

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

rm -Rf /opt/jdk1.8.0_77 /opt/jdk-8u77-linux-x64.tar.gz

echo "[INFO] Downloading 'jdk-8u77'"
wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u77-b03/jdk-8u77-linux-x64.tar.gz" -O /opt/jdk-8u77-linux-x64.tar.gz
if [ $? -ne 0 ]; then
    terminate "Failed to download 'jdk-8u77'"
fi

echo "[INFO] Unzipping 'jdk-8u77'"
tar -xvzf /opt/jdk-8u77-linux-x64.tar.gz -C /opt
if [ $? -ne 0 ]; then
    terminate "Failed to untar 'jdk-8u77'"
fi

rm -Rf /opt/jdk-8u77-linux-x64.tar.gz

downloadPackage "https://bootstrap.pypa.io/get-pip.py" "/opt/get-pip.py" "get-pip.py"

echo "[INFO] Installing 'pip'"
python /opt/get-pip.py
if [ $? -ne 0 ]; then
    terminate "Failed to install 'pip'"
fi

echo "[INFO] Installing 'awscli'"
pip install --upgrade awscli
if [ $? -ne 0 ]; then
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
fi

tagInstance

echo "[INFO] Creating 'cassandra' group"
exists=$(cat /etc/group | grep cassandra)
if [ -z "$exists" ]; then
    groupadd cassandra
    if [ $? -ne 0 ]; then
        terminate "Failed to create 'cassandra' group"
    fi
fi

echo "[INFO] Creating 'cassandra' user"
exists=$(cat /etc/passwd | grep cassandra)
if [ -z "$exists" ]; then
    useradd -g cassandra cassandra
    if [ $? -ne 0 ]; then
        terminate "Failed to create 'cassandra' user"
    fi
fi

rm -Rf /storage/cassandra /opt/cassandra /opt/$CASSANDRA_TARBALL

echo "[INFO] Creating '/storage/cassandra' storage"
mkdir -p /storage/cassandra
chown -R cassandra:cassandra /storage/cassandra
if [ $? -ne 0 ]; then
    terminate "Failed to setup Cassandra storage dir: /storage/cassandra"
fi

downloadPackage "$CASSANDRA_DOWNLOAD_URL" "/opt/$CASSANDRA_TARBALL" "Cassandra"

echo "[INFO] Unzipping Cassandra package"
tar -xvzf /opt/$CASSANDRA_TARBALL -C /opt
if [ $? -ne 0 ]; then
    terminate "Failed to untar Cassandra package"
fi

rm -f /opt/$CASSANDRA_TARBALL /opt/cassandra
mv /opt/$CASSANDRA_UNTAR_DIR /opt/cassandra
chown -R cassandra:cassandra /opt/cassandra

downloadPackage "$TESTS_PACKAGE_DONLOAD_URL" "/opt/$TESTS_PACKAGE_ZIP" "Tests"

unzip /opt/$TESTS_PACKAGE_ZIP -d /opt
if [ $? -ne 0 ]; then
    terminate "Failed to unzip tests package: $TESTS_PACKAGE_DONLOAD_URL"
fi

chown -R cassandra:cassandra /opt/$TESTS_PACKAGE_UNZIP_DIR
find /opt/$TESTS_PACKAGE_UNZIP_DIR -type f -name "*.sh" -exec chmod ug+x {} \;

if [ ! -f "/opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-env.sh" ]; then
    terminate "There are no cassandra-env.sh in tests package"
fi

if [ ! -f "/opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-start.sh" ]; then
    terminate "There are no cassandra-start.sh in tests package"
fi

if [ ! -f "/opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-template.yaml" ]; then
    terminate "There are no cassandra-start.sh in tests package"
fi

if [ ! -f "/opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/logs-collector.sh" ]; then
    terminate "There are no logs-collector.sh in tests package"
fi

mv -f /opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-start.sh /opt
mv -f /opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-env.sh /opt/cassandra/conf
mv -f /opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/cassandra/cassandra-template.yaml /opt/cassandra/conf
mv -f /opt/$TESTS_PACKAGE_UNZIP_DIR/bootstrap/aws/logs-collector.sh /opt
rm -Rf /opt/$TESTS_PACKAGE_UNZIP_DIR
chown -R cassandra:cassandra /opt/cassandra /opt/cassandra-start.sh /opt/logs-collector.sh

#profile=/home/cassandra/.bash_profile
profile=/root/.bash_profile

echo "export JAVA_HOME=/opt/jdk1.8.0_77" >> $profile
echo "export CASSANDRA_HOME=/opt/cassandra" >> $profile
echo "export PATH=\$JAVA_HOME/bin:\$CASSANDRA_HOME/bin:\$PATH" >> $profile
echo "export S3_BOOTSTRAP_SUCCESS_URL=$S3_BOOTSTRAP_SUCCESS_URL" >> $profile
echo "export S3_BOOTSTRAP_FAILURE_URL=$S3_BOOTSTRAP_FAILURE_URL" >> $profile
echo "export S3_CASSANDRA_NODES_DISCOVERY_URL=$S3_CASSANDRA_NODES_DISCOVERY_URL" >> $profile
echo "export S3_CASSANDRA_NODES_JOIN_LOCK_URL=$S3_CASSANDRA_NODES_JOIN_LOCK_URL" >> $profile
echo "export S3_CASSANDRA_FIRST_NODE_LOCK_URL=$S3_CASSANDRA_FIRST_NODE_LOCK_URL" >> $profile

HOST_NAME=$(hostname -f | tr '[:upper:]' '[:lower:]')

/opt/logs-collector.sh "/opt/cassandra/logs" "$S3_LOGS_URL/$HOST_NAME" "$S3_LOGS_TRIGGER_URL" > /opt/cassandra/logs-collector.log &

cmd="/opt/cassandra-start.sh"

#sudo -u cassandra -g cassandra sh -c "$cmd | tee /opt/cassandra/start.log"

$cmd | tee /opt/cassandra/start.log