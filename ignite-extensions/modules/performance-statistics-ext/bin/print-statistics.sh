#!/usr/bin/env bash

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
# The script is used to read performance statistics files to the console or file.
#

SCRIPT_DIR=$(cd $(dirname "$0"); pwd)

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms32m -Xmx512m"

#
# Define classpath
#
CP="${SCRIPT_DIR}/../libs/ignite-performance-statistics-ext/*"

#
# Set main class to run the tool.
#
MAIN_CLASS=org.apache.ignite.internal.performancestatistics.PerformanceStatisticsPrinter

#
# Garbage Collection options.
#
JVM_OPTS="\
    -XX:+UseG1GC \
     ${JVM_OPTS}"

#
# Run tool.
#
java ${JVM_OPTS} -cp "${CP}" ${MAIN_CLASS} $@
