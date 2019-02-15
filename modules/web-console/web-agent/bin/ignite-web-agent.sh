#!/bin/bash
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
# Discover path to Java executable and check it's version.
#
checkJava

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "$JVM_OPTS" ] ; then
    if [[ `"$JAVA" -version 2>&1 | egrep "1\.[7]\."` ]]; then
        JVM_OPTS="-Xms1g -Xmx1g -server -XX:MaxPermSize=256m"
    else
        JVM_OPTS="-Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m"
    fi
fi

# https://confluence.atlassian.com/kb/basic-authentication-fails-for-outgoing-proxy-in-java-8u111-909643110.html
JVM_OPTS="${JVM_OPTS} -Djava.net.useSystemProxies=true -Djdk.http.auth.tunneling.disabledSchemes="

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

"$JAVA" ${JVM_OPTS} -cp "${IGNITE_HOME}/*" org.apache.ignite.console.agent.AgentLauncher "$@"
