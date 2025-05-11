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

###
# DuckerUp parameters are specified with env variables

# Num of cotainers that ducktape will prepare for tests
IGNITE_NUM_CONTAINERS=${IGNITE_NUM_CONTAINERS:-13}

# Image name to run nodes
JDK_VERSION="${JDK_VERSION:-11}"
IMAGE_PREFIX="ducker-ignite-openjdk"

###
# DuckerTest parameters are specified with options to the script

# Path to ducktests
TC_PATHS="./ignitetest/"
# Global parameters to pass to ducktape util with --global param
GLOBALS="{}"
# Ducktests parameters to pass to ducktape util with --parameters param
PARAMETERS="{}"

###
# RunTests parameters
# Force flag:
# - skips ducker-ignite compare step;
# - sends to duck-ignite scripts.
FORCE=

usage() {
    cat <<EOF
run_tests.sh: useful entrypoint to ducker-ignite util

Usage: ${0} [options]

The options are as follows:
-h|--help
    Display this help message.

-n|--num-nodes
    Specify how many nodes to start. Default number of nodes to start: 13 (12 + 1 used by ducktape).

-j|--max-parallel
    Specify max number of tests that can be run in parallel.

-p|--param
    Use specified param to inject in tests. Could be used multiple times.

-pj|--params-json
    Use specified json as parameters to inject in tests. Can be extended with -p|--param.

-g|--global
    Use specified global param to pass to test context. Could be used multiple times.

-gj|--global-json)
    Use specified json as globals to pass to test context. Can be extended with -g|--global

-t|--tc-paths
    Path to ducktests. Must be relative path to 'IGNITE/modules/ducktests/tests' directory

--subnet
    Subnet to assign nodes IP addresses, like --subnet 172.20.0.0/16

--jdk
    Set jdk version to build, default is 11

--image
    Set custom docker image to run tests on.

EOF
    exit 0
}


die() {
    echo "$@"
    exit 1
}

_extend_json() {
    python - "$1" "$2" <<EOF
import sys
import json

[j, key_val] = sys.argv[1:]
[key, val] = key_val.split('=', 1)
j = json.loads(j)
j[key] = val

print(json.dumps(j))

EOF
}

duck_add_global() {
  GLOBALS="$(_extend_json "${GLOBALS}" "${1}")"
}

duck_add_param() {
  PARAMETERS="$(_extend_json "${PARAMETERS}" "${1}")"
}

while [[ $# -ge 1 ]]; do
    case "$1" in
        -h|--help) usage;;
        -p|--param) duck_add_param "$2"; shift 2;;
        -pj|--params-json) PARAMETERS="$2"; shift 2;;
        -g|--global) duck_add_global "$2"; shift 2;;
        -gj|--global-json) GLOBALS="$2"; shift 2;;
        -t|--tc-paths) TC_PATHS="$2"; shift 2;;
        -n|--num-nodes) IGNITE_NUM_CONTAINERS="$2"; shift 2;;
        -j|--max-parallel) MAX_PARALLEL="$2"; shift 2;;
        --subnet) SUBNET="--subnet $2"; shift 2;;
        --jdk) JDK_VERSION="$2"; shift 2;;
        --image) IMAGE_NAME="$2"; shift 2;;
        -f|--force) FORCE=$1; shift;;
        *) break;;
    esac
done

if [ -z "$IMAGE_NAME" ]; then
    IMAGE_NAME="$IMAGE_PREFIX-$JDK_VERSION"
    "$SCRIPT_DIR"/ducker-ignite build -j "openjdk:$JDK_VERSION" $IMAGE_NAME || die "ducker-ignite build failed"
else
    echo "[WARN] Used non-default image $IMAGE_NAME. Be sure you use actual version of the image. " \
         "Otherwise build it with 'ducker-ignite build' command"
fi

if [ -z "$FORCE" ]; then
    # If docker image changed then restart cluster (down here and up within next step)
    "$SCRIPT_DIR"/ducker-ignite compare "$IMAGE_NAME" || die "ducker-ignite compare failed"
fi

# Up cluster if nothing is running
if "$SCRIPT_DIR"/ducker-ignite ssh | grep -q '(none)'; then
    # do not quote FORCE and SUBNET as bash recognize "" as input param instead of image name
    "$SCRIPT_DIR"/ducker-ignite up $FORCE $SUBNET -n "$IGNITE_NUM_CONTAINERS" "$IMAGE_NAME" || die "ducker-ignite up failed"
fi

DUCKTAPE_OPTIONS="--globals '$GLOBALS'"
# If parameters are passed in options than it must contain all possible parameters, otherwise None will be injected
if [[ "$PARAMETERS" != "{}" ]]; then
    DUCKTAPE_OPTIONS="$DUCKTAPE_OPTIONS --parameters '$PARAMETERS'"
fi

if [[ -n "$MAX_PARALLEL" ]]; then
  DUCKTAPE_OPTIONS="$DUCKTAPE_OPTIONS --max-parallel $MAX_PARALLEL"
fi

"$SCRIPT_DIR"/ducker-ignite test $TC_PATHS "$DUCKTAPE_OPTIONS" \
  || die "ducker-ignite test failed"
