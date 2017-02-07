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
# Starts H2 database server and console for Ignite Schema Import demo.
#

# Remember command line parameters
ARGS=$@

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

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS}" == "" ]; then
    DOCK_OPTS="-Dapple.awt.UIElement="true""
fi

# Starts H2 server and console.
case $osname in
    Darwin*)
        "${JAVA}" "${DOCK_OPTS}" -cp "${IGNITE_HOME}/libs/ignite-indexing/h2-1.4.191.jar" org.h2.tools.Console ${ARGS}
        ;;
   *)
        "${JAVA}" -cp "${IGNITE_HOME}/libs/ignite-indexing/h2-1.4.191.jar" org.h2.tools.Console ${ARGS}
        ;;
esac
