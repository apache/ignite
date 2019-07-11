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
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Web Console command line loader.
#

#
# The function exports IGNITE_HOME variable with path to Web Console home directory.
#

setWebConsoleHome() {
    #
    # Set IGNITE_HOME, if needed.
    #
    IGNITE_HOME="$(cd "$(dirname "$0")"; "pwd")";

    #
    # Check IGNITE_HOME is valid.
    #
    if [ ! -d "${IGNITE_HOME}/agent_dists" ]; then
        echo $0", ERROR:"
        echo "Web Console installation folder is incorrect."
        echo "Please run web-console.sh from Web Console installation folder."

        exit 1
    fi
}

setWebConsoleHome

javaVersion() {
    version=$("$1" -version 2>&1 | awk -F '"' '/version/ {print $2}')
}

# Extract only major version of java to `version` variable.
javaMajorVersion() {
    javaVersion "$1"
    version="${version%%.*}"

    if [ ${version} -eq 1 ]; then
        # Version seems starts from 1, we need second number.
        javaVersion "$1"
        backIFS=$IFS

        IFS=. ver=(${version##*-})
        version=${ver[1]}

        IFS=$backIFS
    fi
}

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    # Check JAVA_HOME.
    if [ "${JAVA_HOME:-}" = "" ]; then
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

#
# Discover path to Java executable and check it's version.
#
checkJava

if [ $version -eq 8 ] ; then
    JVM_OPTS="\
        -XX:+AggressiveOpts \
         ${JVM_OPTS}"

elif [ $version -gt 8 ] && [ $version -lt 11 ]; then
    JVM_OPTS="\
        -XX:+AggressiveOpts \
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --illegal-access=permit \
        --add-modules=java.transaction \
        --add-modules=java.xml.bind \
        ${JVM_OPTS}"

elif [ $version -ge 11 ] ; then
    JVM_OPTS="\
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --illegal-access=permit \
        ${JVM_OPTS}"
fi

"$JAVA" -DIGNITE_HOME="${IGNITE_HOME}" ${JVM_OPTS} -jar "${IGNITE_HOME}"/ignite-web-console-*.jar
