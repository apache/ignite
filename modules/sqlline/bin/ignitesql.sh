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
# Ignite database launcher.
#

SELF_NAME=$(basename $0)

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh
source "${SCRIPTS_HOME}"/include/ignitesql-functions.sh

#
# Parse arguments.
#
parse_arguments $@

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

JDBCLINK="jdbc:ignite:thin://${HOST}${PORT_DELIMITER}${PORT}${SCHEMA_DELIMITER}${SCHEMA}${PARAMS}"

CP="${IGNITE_LIBS}"

CP="${CP}${SEP}${IGNITE_HOME_TMP}/bin/include/sqlline/*"

java -cp ${CP} sqlline.SqlLine -d org.apache.ignite.IgniteJdbcThinDriver --color=true --verbose=true --showWarnings=true --showNestedErrs=true -u ${JDBCLINK}
