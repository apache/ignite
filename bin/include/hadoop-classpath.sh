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

# Resolve constants.
HADOOP_DEFAULTS="/etc/default/hadoop"

#
# Resolve the rest of Hadoop environment variables.
#
if [[ -z "${HADOOP_COMMON_HOME}" || -z "${HADOOP_HDFS_HOME}" || -z "${HADOOP_MAPRED_HOME}" ]]; then
    if [ -f "$HADOOP_DEFAULTS" ]; then
        source "$HADOOP_DEFAULTS"
    fi
fi

HADOOP_HOME="${HADOOP_HOME:-"${HADOOP_PREFIX}"}"

if [ -n "${HADOOP_HOME}" ]; then
    HADOOP_COMMON_HOME="${HADOOP_COMMON_HOME:-"${HADOOP_HOME}/share/hadoop/common"}"
    HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-"${HADOOP_HOME}/share/hadoop/hdfs"}"
    HADOOP_MAPRED_HOME="${HADOOP_MAPRED_HOME:-"${HADOOP_HOME}/share/hadoop/mapreduce"}"
fi

if [ ! -d "${HADOOP_COMMON_HOME}" ]; then 
    echo "Hadoop common folder ${HADOOP_COMMON_HOME} not found. Please check HADOOP_COMMON_HOME or HADOOP_HOME environment variable."
    exit 1
fi 

if [ ! -d "${HADOOP_HDFS_HOME}" ]; then 
    echo "Hadoop HDFS folder ${HADOOP_HDFS_HOME} not found. Please check HADOOP_HDFS_HOME or HADOOP_HOME environment variable."
    exit 1
fi 

if [ ! -d "${HADOOP_MAPRED_HOME}" ]; then 
    echo "Hadoop map-reduce folder ${HADOOP_MAPRED_HOME} not found. Please check HADOOP_MAPRED_HOME or HADOOP_HOME environment variable."
    exit 1
fi 


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
