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