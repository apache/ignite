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

    . /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "ganglia"

    echo "----------------------------------------------------------------------------------------"
    printInstanceInfo
    echo "----------------------------------------------------------------------------------------"
    tagInstance
}

setupGangliaPackages()
{
    echo "[INFO] Installing Ganglia required packages"

    yum -y install apr-devel apr-util check-devel cairo-devel pango-devel pango \
    libxml2-devel glib2-devel dbus-devel freetype-devel freetype \
    libpng-devel libart_lgpl-devel fontconfig-devel gcc-c++ expat-devel \
    python-devel libXrender-devel perl-devel perl-CPAN gettext git sysstat \
    automake autoconf ltmain.sh pkg-config gperf libtool pcre-devel libconfuse-devel \
    php-devel php-pear phpize

    if [ $? -ne 0 ]; then
        terminate "Failed to install all Ganglia required packages"
    fi

    echo "[INFO] Installing rrdtool"

    downloadPackage "$RRD_DOWNLOAD_URL" "/opt/rrdtool.tar.gz" "rrdtool"

    tar -xvzf /opt/rrdtool.tar.gz -C /opt
    if [ $? -ne 0 ]; then
        terminate "Failed to untar rrdtool tarball"
    fi

    rm -Rf /opt/rrdtool.tar.gz

    unzipDir=$(ls /opt | grep "rrdtool")
    if [ "$unzipDir" != "rrdtool" ]; then
        mv /opt/$unzipDir /opt/rrdtool
    fi

    export PKG_CONFIG_PATH=/usr/lib/pkgconfig/
    cd /opt/rrdtool

    ./configure --prefix=/usr/local/rrdtool
    if [ $? -ne 0 ]; then
        terminate "Failed to configure rrdtool"
    fi

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make rrdtool"
    fi

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install rrdtool"
    fi

    ln -s /usr/local/rrdtool/bin/rrdtool /usr/bin/rrdtool

    chown -R nobody:nobody /usr/local/rrdtool /usr/bin/rrdtool

    rm -Rf /opt/rrdtool

    echo "[INFO] rrdtool successfully installed"

    echo "[INFO] Installig ganglia-core"

    cd /opt

    git clone $GANGLIA_CORE_DOWNLOAD_URL

    if [ $? -ne 0 ]; then
        terminate "Failed to clone ganglia-core from github: $GANGLIA_CORE_DOWNLOAD_URL"
    fi

    cd /opt/monitor-core

    ./bootstrap

    if [ $? -ne 0 ]; then
        terminate "Failed to prepare ganglia-core for compilation"
    fi

    ./configure --with-gmetad --with-librrd=/usr/local/rrdtool

    if [ $? -ne 0 ]; then
        terminate "Failed to configure ganglia-core"
    fi

    make
    if [ $? -ne 0 ]; then
        terminate "Failed to make ganglia-core"
    fi

    make install
    if [ $? -ne 0 ]; then
        terminate "Failed to install ganglia-core"
    fi

    rm -Rf /opt/monitor-core

    echo "[INFO] ganglia-core successfully installed"

    echo "[INFO] Installing ganglia-web"

    cd /opt

    git clone $GANGLIA_WEB_DOWNLOAD_URL

    if [ $? -ne 0 ]; then
        terminate "Failed to clone ganglia-web from github: $GANGLIA_WEB_DOWNLOAD_URL"
    fi

    echo "" >> /etc/httpd/conf/httpd.conf
    echo "Alias /ganglia /opt/ganglia-web" >> /etc/httpd/conf/httpd.conf
    echo "<Directory \"/opt/ganglia-web\">" >> /etc/httpd/conf/httpd.conf
    echo "       AllowOverride All" >> /etc/httpd/conf/httpd.conf
    echo "       Order allow,deny" >> /etc/httpd/conf/httpd.conf
    echo "       Allow from all" >> /etc/httpd/conf/httpd.conf
    echo "       Deny from none" >> /etc/httpd/conf/httpd.conf
    echo "</Directory>" >> /etc/httpd/conf/httpd.conf

    service httpd start

    if [ $? -ne 0 ]; then
        terminate "Failed to start httpd service"
    fi

    sleep 30s

    exists=$(service httpd status | grep running)
    if [ -z "$exists" ]; then
        terminate "httpd service process terminated"
    fi

    if [ ! -d "/var/lib/ganglia-web/dwoo" ]; then
        mkdir -p /var/lib/ganglia-web/dwoo
        chmod -R a+rw /var/lib/ganglia-web
    fi

    chmod -R a+rw /var/lib/ganglia-web/dwoo

    echo "[INFO] ganglia-web successfully installed"
}

###################################################################################################################

echo "[INFO]-----------------------------------------------------------------"
echo "[INFO] Bootstrapping Ganglia master server"
echo "[INFO]-----------------------------------------------------------------"

exists=$(chkconfig --list | grep httpd)
if [ -z "$exists" ]; then
    echo "[INFO] Installing httpd service"

    yum -y install httpd

    if [ $? -ne 0 ]; then
        terminate "Failed to install httpd service"
    fi
fi

if [ ! -f "/etc/httpd/conf/httpd.conf" ]; then
    terminate "It's not possible to install Ganglia in this host cause it doens't have /etc/httpd/conf/httpd.conf"
fi

setupPreRequisites
setupJava
setupAWSCLI
setupTestsPackage
setupGangliaPackages

registerNode

setupClusterSeeds "cassandra"
CASSANDRA_SEEDS=$CLUSTER_SEEDS

setupClusterSeeds "ignite"
IGNITE_SEEDS=$CLUSTER_SEEDS

setupClusterSeeds "test"
TEST_SEEDS=$CLUSTER_SEEDS

echo "data_source \"cassandra\" $CASSANDRA_SEEDS" > /opt/gmetad.conf
echo "data_source \"ignite\" $IGNITE_SEEDS" >> /opt/gmetad.conf
echo "data_source \"tests\" $TEST_SEEDS" >> /opt/gmetad.conf
echo "setuid_username \"nobody\"" >> /opt/gmetad.conf
echo "case_sensitive_hostnames 0" >> /opt/gmetad.conf

echo "[INFO] Starting gmetad daemon"

gmetad --conf=/opt/gmetad.conf

if [ $? -ne 0 ]; then
    terminate "Failed to start gmetad daemon"
fi

sleep 10s

exists=$(ps -ef | grep gmetad)

if [ -z "$exists" ]; then
    terminate "gmetad daemon process terminated"
fi

echo "[INFO] gmetad daemon successfully started"

terminate
