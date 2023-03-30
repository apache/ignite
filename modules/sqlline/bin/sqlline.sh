#!/usr/bin/env bash
if [ ! -z "${IGNITE_SCRIPT_STRICT_MODE:-}" ]
then
    set -o nounset
    set -o errexit
    set -o pipefail
    set -o errtrace
    set -o functrace
fi

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
# Ignite database connector.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME:-}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh
source "${SCRIPTS_HOME}"/include/jvmdefaults.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

JVM_OPTS=${JVM_OPTS:-}

#
# Final JVM_OPTS for Java 9+ compatibility
#
JVM_OPTS=$(getJavaSpecificOpts $version "$JVM_OPTS")

JDBCLINK="jdbc:ignite:thin://${HOST_AND_PORT:-}${SCHEMA_DELIMITER:-}${SCHEMA:-}${PARAMS:-}"

CP="${IGNITE_LIBS}"

CP="${CP}${SEP}${IGNITE_HOME_TMP}/bin/include/sqlline/*"

# Between version 2 and 3, jline changed the format of its history file. After this change,
# the Ignite provides --historyfile argument to SQLLine usage
SQLLINE_HISTORY="~/.sqlline/ignite_history"

"$JAVA" ${JVM_OPTS} -cp ${CP} sqlline.SqlLine --historyFile=${SQLLINE_HISTORY} "$@"
