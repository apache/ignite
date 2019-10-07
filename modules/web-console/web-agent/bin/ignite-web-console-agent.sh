#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

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

SOURCE="${BASH_SOURCE[0]}"

# Resolve $SOURCE until the file is no longer a symlink.
while [ -h "$SOURCE" ]
    do
        IGNITE_HOME="$(cd -P "$( dirname "$SOURCE"  )" && pwd)"

        SOURCE="$(readlink "$SOURCE")"

        # If $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located.
        [[ $SOURCE != /* ]] && SOURCE="$IGNITE_HOME/$SOURCE"
    done

#
# Set IGNITE_HOME.
#
export IGNITE_HOME="$(cd -P "$( dirname "$SOURCE" )" && pwd)"

source "${IGNITE_HOME}"/include/functions.sh

#
# OS specific support.
#
getClassPathSeparator;

#
# Libraries included in classpath.
#
IGNITE_LIBS="${IGNITE_HOME}/*${SEP}${IGNITE_HOME}/libs/*"

for file in ${IGNITE_HOME}/libs/*
do
    if [ -d ${file} ] && [ "${file}" != "${IGNITE_HOME}"/libs/optional ]; then
        IGNITE_LIBS=${IGNITE_LIBS:-}${SEP}${file}/*
    fi
done

if [ "${USER_LIBS:-}" != "" ]; then
    IGNITE_LIBS=${USER_LIBS:-}${SEP}${IGNITE_LIBS}
fi

CP="${IGNITE_LIBS}"

#
# Discover path to Java executable and check it's version.
#
checkJava

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS:-}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Ignite Node"
fi

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "${JVM_OPTS:-}" ] ; then
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m"
fi

#
# Set 'file.encoding' to UTF-8 default if not specified otherwise
#
case "${JVM_OPTS:-}" in
    *-Dfile.encoding=*)
        ;;
    *)
        JVM_OPTS="${JVM_OPTS:-} -Dfile.encoding=UTF-8";;
esac

# https://confluence.atlassian.com/kb/basic-authentication-fails-for-outgoing-proxy-in-java-8u111-909643110.html
JVM_OPTS="${JVM_OPTS} -Djava.net.useSystemProxies=true -Djdk.http.auth.tunneling.disabledSchemes="

#
# Set main class to start service (grid node by default).
#
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.console.agent.AgentLauncher
fi

#
# Final JVM_OPTS for Java 9+ compatibility
#
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

case $osname in
    Darwin*)
        "$JAVA" ${JVM_OPTS} "${DOCK_OPTS}" -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    *)
        "$JAVA" ${JVM_OPTS} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
esac
