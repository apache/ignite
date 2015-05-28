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
# Starts Ignite Schema Import Utility.
#

# Remember command line parameters
ARGS=$@

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

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Parse command line parameters.
#
. "${SCRIPTS_HOME}"/include/parseargs.sh

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh
. "${SCRIPTS_HOME}"/include/build-classpath.sh # Will be removed in the binary release.
CP="${JAVA_HOME}/jre/lib/jfxrt.jar${SEP}${IGNITE_HOME}/bin/include/schema-import/*${SEP}${IGNITE_LIBS}"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms256m -Xmx1g -XX:MaxPermSize=128M ${JVM_OPTS}"

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Ignite Schema Import Utility"
fi

#
# Starts Ignite Schema Import Utility.
#
case $osname in
    Darwin*)
        "$JAVA" ${JVM_OPTS} "${DOCK_OPTS}" -cp "${CP}" org.apache.ignite.schema.ui.SchemaImportApp ${ARGS}
        ;;
   *)
        "$JAVA" ${JVM_OPTS} -cp "${CP}" org.apache.ignite.schema.ui.SchemaImportApp ${ARGS}
        ;;
esac
