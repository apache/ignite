#!/bin/bash
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

DISTR_DIR="$(pwd)/distr/"
SRC_DIR="$(pwd)"
DEFAULT_DOCKER_IMAGE="quay.io/pypa/manylinux2014_x86_64"

usage() {
    cat <<EOF
create_distr.sh: creates wheels and source distr for different python versions and platforms.

Usage: ${0} [options]

The options are as follows:
-h|--help
    Display this help message.

-a|--arch
    Specify architecture, supported variants: i686,x86,x86_64. Build all supported by default.

-d|--dir
    Specify directory where to store artifacts. Default $(PWD)/../distr

EOF
    exit 0
}

normalize_path() {
    mkdir -p "$DISTR_DIR"
    cd "$DISTR_DIR" || exit 1
    DISTR_DIR="$(pwd)"
    cd "$SRC_DIR" || exit 1
    SRC_DIR="$(pwd)"
}

run_wheel_arch() {
    if [[ $1 =~ ^(i686|x86)$ ]]; then
        PLAT="manylinux1_i686"
        PRE_CMD="linux32"
        DOCKER_IMAGE="quay.io/pypa/manylinux2014_i686"
    elif [[ $1 =~ ^(x86_64)$ ]]; then
        PLAT="manylinux1_x86_64"
        PRE_CMD=""
        DOCKER_IMAGE="$DEFAULT_DOCKER_IMAGE"
    else
        echo "unsupported architecture $1, only x86(i686) and x86_64 supported"
        exit 1
    fi

    WHEEL_DIR="$DISTR_DIR/$1"
    mkdir -p "$WHEEL_DIR"
    docker run --rm -e PLAT=$PLAT -v "$SRC_DIR":/pyignite -v "$WHEEL_DIR":/wheels $DOCKER_IMAGE $PRE_CMD /pyignite/scripts/build_wheels.sh
}

while [[ $# -ge 1 ]]; do
    case "$1" in
        -h|--help) usage;;
        -a|--arch) ARCH="$2"; shift 2;;
        -d|--dir) DISTR_DIR="$2"; shift 2;;
        *) break;;
    esac
done

normalize_path

docker run --rm -v "$SRC_DIR":/pyignite -v "$DISTR_DIR":/dist $DEFAULT_DOCKER_IMAGE /pyignite/scripts/create_sdist.sh

if [[ -n "$ARCH" ]]; then
    run_wheel_arch "$ARCH"
else
    run_wheel_arch "x86"
    run_wheel_arch "x86_64"
fi
