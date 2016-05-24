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

bootstrapGangliaAgent()
{
    echo "[INFO]-----------------------------------------------------------------"
    echo "[INFO] Bootstrapping Ganglia agent"
    echo "[INFO]-----------------------------------------------------------------"

    echo "[INFO] Installing Ganglia agent required packages"

    yum -y install apr-devel apr-util check-devel cairo-devel pango-devel pango \
    libxml2-devel glib2-devel dbus-devel freetype-devel freetype \
    libpng-devel libart_lgpl-devel fontconfig-devel gcc-c++ expat-devel \
    python-devel libXrender-devel perl-devel perl-CPAN gettext git sysstat \
    automake autoconf ltmain.sh pkg-config gperf libtool pcre-devel libconfuse-devel

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
    pushd /opt/rrdtool

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

    popd

    echo "[INFO] rrdtool successfully installed"

    echo "[INFO] Installig ganglia-core"

    pushd /opt

    git clone $GANGLIA_CORE_DOWNLOAD_URL

    if [ $? -ne 0 ]; then
        terminate "Failed to clone ganglia-core from github: $GANGLIA_CORE_DOWNLOAD_URL"
    fi

    popd

    pushd /opt/monitor-core

    ./bootstrap

    if [ $? -ne 0 ]; then
        terminate "Failed to prepare ganglia-core for compilation"
    fi

    ./configure --with-librrd=/usr/local/rrdtool

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

    #rm -Rf /opt/monitor-core

    sleep 10s

    popd

    echo "[INFO] ganglia-core successfully installed"

    echo "[INFO] Running ganglia agent daemon to discover Ganglia master"

    /opt/ignite-cassandra-tests/bootstrap/aws/ganglia/agent-start.sh $1 > /opt/ganglia-agent.log &

    echo "[INFO] Ganglia daemon job id: $!"
}