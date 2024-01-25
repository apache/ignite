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
# Grid cluster control.
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

if [ "${DEFAULT_CONFIG:-}" == "" ]; then
    DEFAULT_CONFIG=config/default-config.xml
fi

#
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

CP="${IGNITE_LIBS}:${IGNITE_HOME}/libs/optional/ignite-zookeeper/*"

RANDOM_NUMBER=$("$JAVA" -cp "${CP}" org.apache.ignite.startup.cmdline.CommandLineRandomNumberGenerator)

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
if [ -z "${CONTROL_JVM_OPTS:-}" ] ; then
    if [[ `"$JAVA" -version 2>&1 | egrep "1\.[7]\."` ]]; then
        CONTROL_JVM_OPTS="-Xms256m -Xmx1g"
    else
        CONTROL_JVM_OPTS="-Xms256m -Xmx1g"
    fi
fi

#
# Uncomment to enable experimental commands [--wal]
#
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -DIGNITE_ENABLE_EXPERIMENTAL_COMMAND=true"

#
# Uncomment the following GC settings if you see spikes in your throughput due to Garbage Collection.
#
# CONTROL_JVM_OPTS="$CONTROL_JVM_OPTS -XX:+UseG1GC"

#
# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
#
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -Xss4m"

#
# Uncomment to set preference for IPv4 stack.
#
# CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -Djava.net.preferIPv4Stack=true"

#
# Assertions are disabled by default since version 3.5.
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="0"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS:-}" = "1" ]; then
    CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} -ea"
fi

#
# Set main class to start service (grid node by default).
#
if [ "${MAIN_CLASS:-}" = "" ]; then
    MAIN_CLASS=org.apache.ignite.internal.commandline.CommandHandler
fi

#
# Remote debugging (JPDA).
# Uncomment and change if remote debugging is required.
#
# CONTROL_JVM_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8787 ${CONTROL_JVM_OPTS}"

#
# Final CONTROL_JVM_OPTS for Java 9+ compatibility
#
CONTROL_JVM_OPTS=$(getJavaSpecificOpts $version "$CONTROL_JVM_OPTS")
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/java.io=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/java.nio=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/java.util=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --add-opens=java.base/java.lang=ALL-UNNAMED"
CONTROL_JVM_OPTS="${CONTROL_JVM_OPTS} --illegal-access=warn"

if [ -n "${JVM_OPTS}" ] ; then
  echo "JVM_OPTS environment variable is set, but will not be used. To pass JVM options use CONTROL_JVM_OPTS"
  echo "JVM_OPTS=${JVM_OPTS}"
fi

case $osname in
    Darwin*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} "${DOCK_OPTS}" \
          -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    OS/390*)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} \
          -DIGNITE_HOME="${IGNITE_HOME}" \
         -Dcom.ibm.jsse2.overrideDefaultTLS=true -Dssl.KeyManagerFactory.algorithm=IbmX509 \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
    *)
        "$JAVA" ${CONTROL_JVM_OPTS} ${QUIET:-} \
          -DIGNITE_HOME="${IGNITE_HOME}" \
         -DIGNITE_PROG_NAME="$0" ${JVM_XOPTS:-} -cp "${CP}" ${MAIN_CLASS} "$@"
    ;;
esac
