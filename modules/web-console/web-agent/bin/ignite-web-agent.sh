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

"$JAVA" ${JVM_OPTS} -cp "${IGNITE_HOME}/*" org.apache.ignite.console.agent.AgentLauncher "$@"
