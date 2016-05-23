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

. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "ganglia"

echo "[INFO] Running Ganglia agent discovery daemon for '$1' cluster"

waitFirstClusterNodeRegistered

DISCOVERY_URL=$(getDiscoveryUrl)

masterNode=$(aws s3 ls $DISCOVERY_URL | sed -r "s/PRE//g" | sed -r "s/\///g" | head -1 | xargs)

if [ $? -ne 0 ] || [ -z "$masterNode" ]; then
    echo "[ERROR] Failed to get Ganglia master node from: $DISCOVERY_URL"
fi

echo "[INFO] Got Ganglia master node: $masterNode"

echo "[INFO] Creating gmond config file"

gmond --default_config > /opt/gmond.conf

cat /opt/gmond.conf | sed -r "s/deaf = no/deaf = yes/g" | sed -r "s/name = \"unspecified\"/name = \"$1\"/g" | sed -r "s/#bind_hostname/bind_hostname/g" | sed -r "s/mcast_join = 239.2.11.71/host = $masterNode/g" | sed -r "s/bind = 239.2.11.71//g" > /opt/gmond1.conf
rm -r /opt/gmond.conf
mv /opt/gmond1.conf /opt/gmond.conf

echo "[INFO] Running gmond daemon to report to gmetad on $masterNode"

gmond --conf=/opt/gmond.conf

sleep 10s

exists=$(ps -ef | grep gmond)

if [ -z "$exists" ]; then
    echo "[ERROR] gmond daemon abnormally terminated"
    exit 1
fi

echo "[ERROR] gmond daemon successfully started"