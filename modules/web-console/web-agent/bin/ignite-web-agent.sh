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

# Check JAVA_HOME.
if [ "$JAVA_HOME" = "" ]; then
    JAVA=`type -p java`
    RETCODE=$?

    if [ $RETCODE -ne 0 ]; then
        echo $0", ERROR:"
        echo "JAVA_HOME environment variable is not found."
        echo "Please point JAVA_HOME variable to location of JDK 1.7 or JDK 1.8."
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
if [ ! -e "$JAVA" ]; then
    echo $0", ERROR:"
    echo "JAVA is not found in JAVA_HOME=$JAVA_HOME."
    echo "Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8."
    echo "You can also download latest JDK at http://java.com/download"

    exit 1
fi

JAVA_VER=`"$JAVA" -version 2>&1 | egrep "1\.[78]\."`

if [ "$JAVA_VER" == "" ]; then
    echo $0", ERROR:"
    echo "The version of JAVA installed in JAVA_HOME=$JAVA_HOME is incorrect."
    echo "Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8."
    echo "You can also download latest JDK at http://java.com/download"

    exit 1
fi

SOURCE="${BASH_SOURCE[0]}"

DIR="$( dirname "$SOURCE" )"

while [ -h "$SOURCE" ]
    do
        SOURCE="$(readlink "$SOURCE")"

        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"

        DIR="$( cd -P "$( dirname "$SOURCE"  )" && pwd )"
    done

DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

cd $DIR

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "$JVM_OPTS" ] ; then
    if [[ `"$JAVA" -version 2>&1 | egrep "1\.[7]\."` ]]; then
        JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxPermSize=256m"
    else
        JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxMetaspaceSize=256m"
    fi
fi

JVM_OPTS="${JVM_OPTS} -Djava.net.useSystemProxies=true"

"$JAVA" ${JVM_OPTS} -cp "*" org.apache.ignite.console.agent.AgentLauncher "$@"
