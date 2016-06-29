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

####################################################################
#                 Hadoop class path resolver.
#  Requires environment variables 'HADOOP_PREFIX' or 'HADOOP_HOME'
#  to be set. If they are both undefined , tries to read them from
#  from '/etc/default/hadoop' file. The final results are printed
#  into standard output.
####################################################################

# Resolve constants.
HADOOP_DEFAULTS="/etc/default/hadoop"
HADOOP_PREFIX=${HADOOP_PREFIX:-$HADOOP_HOME}

# Try get all variables from Hadoop default environment config
# if they have not been passed into the script.
if [[ -z "$HADOOP_PREFIX" && -f "$HADOOP_DEFAULTS" ]]; then
    source "$HADOOP_DEFAULTS"
fi

# Return if Hadoop couldn't be found.
[ -z "$HADOOP_PREFIX" ] && return

#
# Resolve the rest of Hadoop environment variables.
#

HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME-"${HADOOP_PREFIX}/share/hadoop/common"}
HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME-"${HADOOP_PREFIX}/share/hadoop/hdfs"}
HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME-"${HADOOP_PREFIX}/share/hadoop/mapreduce"}

#
# Calculate classpath string with required Hadoop libraries.
#

# Add all Hadoop libs.
IGNITE_HADOOP_CLASSPATH="${HADOOP_COMMON_HOME}/lib/*${SEP}${HADOOP_MAPRED_HOME}/lib/*${SEP}${HADOOP_MAPRED_HOME}/lib/*"

# Skip globbing pattern if it cannot be resolved.
shopt -s nullglob

# Add jars to classpath excluding tests.
# hadoop-auth-* jar can be located either in home or in home/lib directory, depending on the hadoop version.
for file in ${HADOOP_HDFS_HOME}/hadoop-hdfs-* \
            ${HADOOP_COMMON_HOME}/hadoop-{common,auth}-* \
            ${HADOOP_COMMON_HOME}/lib/hadoop-auth-* \
            ${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-{common,core}-*; do
    [[ "$file" != *-tests.jar ]] && IGNITE_HADOOP_CLASSPATH=${IGNITE_HADOOP_CLASSPATH}${SEP}${file}
done
