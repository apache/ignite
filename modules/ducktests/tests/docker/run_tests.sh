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
IGNITE_NUM_CONTAINERS=${IGNITE_NUM_CONTAINERS:-6}
TC_PATHS=${TC_PATHS:-./ignitetest/tests/spark_integration_test.py}

die() {
    echo $@
    exit 1
}

if ${SCRIPT_DIR}/ducker-ignite ssh | grep -q '(none)'; then
    ${SCRIPT_DIR}/ducker-ignite up -n "${IGNITE_NUM_CONTAINERS}" || die "ducker-ignite up failed"
fi
${SCRIPT_DIR}/ducker-ignite test ${TC_PATHS} ${_DUCKTAPE_OPTIONS} || die "ducker-ignite test failed"
