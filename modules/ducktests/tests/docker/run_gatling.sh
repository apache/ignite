#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

GATLING_STARTER="/ignitetest/gatling/gatling_test.py"
GATLING_SIMULATION_CLASS="org.apache.ignite.gatling.simulations.CachePutSimulation"
SERVER_NODES_COUNT=1
GATLING_NODES_COUNT=1

usage() {
    cat <<EOF
run_gutling.sh: tool to run Gatling tests

Usage: ${0} [options]

The options are as follows:
-h|--help
    Display this help message.

-srv|--servers-nodes-count
    Specify how many server nodes to start.

-g|--gatling-nodes-count
    Specify how many Gatling nodes to start.

-s|--simulation
    Specify test simulation.
EOF
    exit 0
}

die() {
    echo "$@"
    exit 1
}

while [[ $# -ge 1 ]]; do
    case "$1" in
        -h|--help) usage;;
        -srv|--servers-nodes-count) SERVER_NODES_COUNT="$2"; shift 2;;
        -g|--gatling-nodes-count) GATLING_NODES_COUNT="$2"; shift 2;;
        -s|--simulation) GATLING_SIMULATION_CLASS="$2"; shift 2;;
        *) break;;
    esac
done

"$SCRIPT_DIR"/run_tests.sh \
  -t "$GATLING_STARTER" \
  -g "gatling_simulation_class=${GATLING_SIMULATION_CLASS}" \
  -g "server_nodes_count=${SERVER_NODES_COUNT}" \
  -g "gatling_nodes_count=${GATLING_NODES_COUNT}" \
  || die "Failed to run Gatling test"

echo "Creating Gatling report."

"$SCRIPT_DIR"/ducker-ignite ssh ducker01 "/opt/ignite-dev/modules/gatling/target/gatling-charts-highcharts-bundle-3.5.1/bin/gatling.sh -ro /opt/ignite-dev/results/latest/GatlingTest/gatling_test/ignite_version=ignite-dev/1" \
    || die "Failed to create report."
