#!/bin/bash
#
# @sh.file.header
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: @sh.file.version
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
GRIDGAIN_HADOOP_CLASSPATH="${HADOOP_COMMON_HOME}/lib/*${SEP}${HADOOP_MAPRED_HOME}/lib/*${SEP}${HADOOP_MAPRED_HOME}/lib/*"

# Skip globbing pattern if it cannot be resolved.
shopt -s nullglob

# Add jars to classpath excluding tests.
# hadoop-auth-* jar can be located either in home or in home/lib directory, depending on the hadoop version.
for file in ${HADOOP_HDFS_HOME}/hadoop-hdfs-* \
            ${HADOOP_COMMON_HOME}/hadoop-{common,auth}-* \
            ${HADOOP_COMMON_HOME}/lib/hadoop-auth-* \
            ${HADOOP_MAPRED_HOME}/hadoop-mapreduce-client-{common,core}-*; do
    [[ "$file" != *-tests.jar ]] && GRIDGAIN_HADOOP_CLASSPATH=${GRIDGAIN_HADOOP_CLASSPATH}${SEP}${file}
done
