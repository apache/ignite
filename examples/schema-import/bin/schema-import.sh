#!/bin/bash
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

#
# Starts Ignite Schema Import Wizard with predefined settings for demo.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")/../.."; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

# Starts Ignite Schema Import Wizard with settings for this demo.
"${IGNITE_HOME}/bin/ignite-schema-import.sh" \
 jdbc.db.preset=0 \
 jdbc.driver.jar="${IGNITE_HOME}/libs/ignite-indexing/h2-1.3.175.jar" \
 jdbc.driver.class=org.h2.Driver \
 jdbc.url="jdbc:h2:tcp://localhost/${IGNITE_HOME}/examples/schema-import/demo" \
 jdbc.user=sa \
 out.folder="${IGNITE_HOME}/examples/schema-import/src/main/java" \
 pojo.package=org.apache.ignite \
 pojo.constructor=true
