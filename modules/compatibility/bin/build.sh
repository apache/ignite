#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

if [ "${M2_HOME}" = "" ]; then
    MVN_CMD=mvn
else
    MVN_CMD=${M2_HOME}/bin/mvn
fi

#Install custom source Maven plugin
COMPATIBILITY_MODULE_DIR="$(dirname ${SCRIPT_DIR})"

PLUGIN_DIR=$COMPATIBILITY_MODULE_DIR/ignite-filter-source-plugin

CMD="${MVN_CMD} -f $PLUGIN_DIR/pom.xml clean install"

echo "Executing: "${CMD}

${CMD}

ERROR=$?

if [ ${ERROR} -ne 0 ]; then
    exit 1
fi

for file in $SCRIPT_DIR/../ignite-versions/*
do
    if [ -d ${file} ] && [ "$(basename ${file})" != "base" ]; then
        CMD="${MVN_CMD} -s $SCRIPT_DIR/settings.xml -f ${file}/pom.xml clean install"

        echo "Executing: "${CMD}

        ${CMD}

        ERROR=$?

        if [ ${ERROR} -ne 0 ]; then
            exit 1
        fi
    fi
done
