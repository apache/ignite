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
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxPermSize=256m"
fi

java ${JVM_OPTS} -jar ignite-web-agent-${version}.jar "$@"
