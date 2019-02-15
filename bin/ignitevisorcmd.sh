#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
#

# Remember command line parameters
ARGS=$@

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
CP="${IGNITE_HOME}/bin/include/visor-common/*${SEP}${IGNITE_HOME}/bin/include/visorcmd/*${SEP}${IGNITE_LIBS}"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms1g -Xmx1g -XX:MaxPermSize=128M -server ${JVM_OPTS}"

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS:-}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Visor - Ignite Shell Console"
fi

#
# Uncomment to set preference for IPv4 stack.
#
# JVM_OPTS="${JVM_OPTS} -Djava.net.preferIPv4Stack=true"

#
# Assertions are disabled by default since version 3.5.
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="1"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS}" = "1" ]; then
    JVM_OPTS="${JVM_OPTS} -ea"
fi

#
# Save terminal setting. Used to restore terminal on finish.
#
SAVED_STTY=`stty -g 2>/dev/null`

#
# Restores terminal.
#
function restoreSttySettings() {
    stty ${SAVED_STTY}
}

#
# Trap that restores terminal in case script execution is interrupted.
#
trap restoreSttySettings INT

#
# Final JVM_OPTS for Java 9+ compatibility
#
javaMajorVersion "${JAVA_HOME}/bin/java"

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

elif [ $version -eq 11 ] ; then
    JVM_OPTS="\
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --illegal-access=permit \
        ${JVM_OPTS}"
fi

#
# Start Visor console.
#
case $osname in
    Darwin*)
        "$JAVA" ${JVM_OPTS} ${QUIET} "${DOCK_OPTS}" \
        -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" -DIGNITE_PROG_NAME="$0" \
        -DIGNITE_DEPLOYMENT_MODE_OVERRIDE=ISOLATED ${JVM_XOPTS} -cp "${CP}" \
        org.apache.ignite.visor.commands.VisorConsole ${ARGS}
    ;;
    *)
        "$JAVA" ${JVM_OPTS} ${QUIET} -DIGNITE_UPDATE_NOTIFIER=false \
        -DIGNITE_HOME="${IGNITE_HOME}" -DIGNITE_PROG_NAME="$0" -DIGNITE_DEPLOYMENT_MODE_OVERRIDE=ISOLATED \
        ${JVM_XOPTS} -cp "${CP}" \
        org.apache.ignite.visor.commands.VisorConsole ${ARGS}
    ;;
esac

#
# Restore terminal.
#
restoreSttySettings
