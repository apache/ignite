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
IGNITE_NUM_CONTAINERS=${IGNITE_NUM_CONTAINERS:-11}

# Num of containers that ducktape will prepare for tests
IGNITE_NUM_CONTAINERS=${IGNITE_NUM_CONTAINERS:-6}
# Path for tests to start
TC_PATHS=${TC_PATHS:-./ignitetest/}
# Docker image name that ducktape will use to prepare containers
IMAGE_NAME=${IMAGE_NAME}

die() {
    echo $@
    exit 1
}

if ${SCRIPT_DIR}/ducker-ignite ssh | grep -q '(none)'; then
    # If image name is specified that skip build and just pull it
    if [ "$IMAGE_NAME" != "" ]; then
      IMAGE_NAME=" --skip-build-image $IMAGE_NAME"
    fi
    ${SCRIPT_DIR}/ducker-ignite up -n "${IGNITE_NUM_CONTAINERS}" ${IMAGE_NAME} || die "ducker-ignite up failed"
fi

if [ "$VERSION" != "" ]; then
  export _DUCKTAPE_OPTIONS="$_DUCKTAPE_OPTIONS --parameters '{\"version\":\"${VERSION}\"}'"
fi

${SCRIPT_DIR}/ducker-ignite test ${TC_PATHS} ${_DUCKTAPE_OPTIONS} || die "ducker-ignite test failed"
