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

#
# Exports IGNITE_LIBS variable containing classpath for Ignite.
# Expects IGNITE_HOME to be set.
# Can be used like:
#       . "${IGNITE_HOME}"/bin/include/setenv.sh
# in other scripts to set classpath using exported IGNITE_LIBS variable.
#

#
# Check IGNITE_HOME.
#
if [ "${IGNITE_HOME}" = "" ]; then
    echo $0", ERROR: Ignite installation folder is not found."
    echo "Please create IGNITE_HOME variable pointing to location of"
    echo "Ignite installation folder."

    exit 1
fi

#
# OS specific support.
#
SEP=":";

case "`uname`" in
    MINGW*)
        SEP=";";
        export IGNITE_HOME=`echo $IGNITE_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
        ;;
    CYGWIN*)
        SEP=";";
        export IGNITE_HOME=`echo $IGNITE_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
        ;;
esac

#
# Libraries included in classpath.
#
IGNITE_LIBS="${IGNITE_HOME}/libs/*"

for file in ${IGNITE_HOME}/libs/*
do
    if [ -d ${file} ] && [ "${file}" != "${IGNITE_HOME}"/libs/optional ]; then
        IGNITE_LIBS=${IGNITE_LIBS}${SEP}${file}/*
    fi

    if [ -d ${file} ] && [ "${file}" == "${IGNITE_HOME}"/libs/ignite-hadoop ]; then
        HADOOP_EDITION=1
    fi
done

if [ "${USER_LIBS}" != "" ]; then
    IGNITE_LIBS=${USER_LIBS}${SEP}${IGNITE_LIBS}
fi

if [ "${HADOOP_EDITION}" == "1" ]; then
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

    IGNITE_HADOOP_CLASSPATH=$( "$JAVA" -cp "${IGNITE_HOME}"/libs/ignite-hadoop/'*' \
        org.apache.ignite.internal.processors.hadoop.HadoopClasspathMain ":" )

    statusCode=${?}

    if [ "${statusCode}" -ne 0 ]; then
       exit ${statusCode}
    fi

    unset statusCode

    IGNITE_LIBS=${IGNITE_LIBS}${SEP}${IGNITE_HADOOP_CLASSPATH}
fi
