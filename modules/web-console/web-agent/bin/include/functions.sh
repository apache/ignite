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
# This is a collection of utility functions to be used in other Ignite scripts.
# Before calling any function from this file you have to import it:
#   if [ "${IGNITE_HOME}" = "" ];
#       then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
#       else IGNITE_HOME_TMP=${IGNITE_HOME};
#   fi
#
#   source "${IGNITE_HOME_TMP}"/bin/include/functions.sh
#

# Extract java version to `version` variable.
javaVersion() {
    version=$("$1" -version 2>&1 | awk -F[\"\-] '/version/ {print $2}')
}

# Extract only major version of java to `version` variable.
javaMajorVersion() {
    javaVersion "$1"
    version="${version%%.*}"

    if [ ${version} -eq 1 ]; then
        # Version seems starts from 1, we need second number.
        javaVersion "$1"
        version=$(awk -F[\"\.] '{print $2}' <<< ${version})
    fi
}

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    # Check JAVA_HOME.
    if [ "$JAVA_HOME" = "" ]; then
        JAVA=`type -p java`
        RETCODE=$?

        if [ $RETCODE -ne 0 ]; then
            echo $0", ERROR:"
            echo "JAVA_HOME environment variable is not found."
            echo "Please point JAVA_HOME variable to location of JDK 1.8 or later."
            echo "You can also download latest JDK at http://java.com/download"

            exit 1
        fi

        JAVA_HOME=
    else
        JAVA=${JAVA_HOME}/bin/java
    fi

    #
    # Check JDK.
    #
    javaMajorVersion "$JAVA"

    if [ $version -lt 8 ]; then
        echo "$0, ERROR:"
        echo "The $version version of JAVA installed in JAVA_HOME=$JAVA_HOME is incompatible."
        echo "Please point JAVA_HOME variable to installation of JDK 1.8 or later."
        echo "You can also download latest JDK at http://java.com/download"
        exit 1
    fi
}
