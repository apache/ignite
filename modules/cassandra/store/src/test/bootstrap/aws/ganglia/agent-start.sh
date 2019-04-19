#!/bin/sh
#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
# 
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -----------------------------------------------------------------------------------------------
# Script to start Ganglia agent on EC2 node (used by agent-bootstrap.sh)
# -----------------------------------------------------------------------------------------------

. /opt/ignite-cassandra-tests/bootstrap/aws/common.sh "ganglia"

echo "[INFO] Running Ganglia agent discovery daemon for '$1' cluster using $2 port"

# Waiting for the Ganglia master node up and running
waitFirstClusterNodeRegistered

DISCOVERY_URL=$(getDiscoveryUrl)

masterNode=$(aws s3 ls $DISCOVERY_URL | head -1)
masterNode=($masterNode)
masterNode=${masterNode[3]}
masterNode=$(echo $masterNode | xargs)

if [ $? -ne 0 ] || [ -z "$masterNode" ]; then
    echo "[ERROR] Failed to get Ganglia master node from: $DISCOVERY_URL"
fi

echo "[INFO] Got Ganglia master node: $masterNode"

echo "[INFO] Creating gmond config file"

/usr/local/sbin/gmond --default_config > /opt/gmond-default.conf

cat /opt/gmond-default.conf | sed -r "s/deaf = no/deaf = yes/g" | \
sed -r "s/name = \"unspecified\"/name = \"$1\"/g" | \
sed -r "s/#bind_hostname/bind_hostname/g" | \
sed "0,/mcast_join = 239.2.11.71/s/mcast_join = 239.2.11.71/host = $masterNode/g" | \
sed -r "s/mcast_join = 239.2.11.71//g" | sed -r "s/bind = 239.2.11.71//g" | \
sed -r "s/port = 8649/port = $2/g" | sed -r "s/retry_bind = true//g" > /opt/gmond.conf

echo "[INFO] Running gmond daemon to report to gmetad on $masterNode"

/usr/local/sbin/gmond --conf=/opt/gmond.conf -p /opt/gmond.pid

sleep 2s

if [ ! -f "/opt/gmond.pid" ]; then
    echo "[ERROR] Failed to start gmond daemon, pid file doesn't exist"
    exit 1
fi

pid=$(cat /opt/gmond.pid)

echo "[INFO] gmond daemon started, pid=$pid"

exists=$(ps $pid | grep gmond)

if [ -z "$exists" ]; then
    echo "[ERROR] gmond daemon abnormally terminated"
    exit 1
fi