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
# Run this script to configure Hadoop client to work with Ignite.
#

HADOOP_COMMON_HOME=

if [ "$HADOOP_HOME" == "" ]; then
    #Try get all variables from /etc/default
    HADOOP_DEFAULTS=/etc/default/hadoop

    if [ -f $HADOOP_DEFAULTS ]; then
        . $HADOOP_DEFAULTS
    fi
fi

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ]; then 
    IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")"
else 
    IGNITE_HOME_TMP=${IGNITE_HOME}
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set utility environment.
#
export MAIN_CLASS=org.apache.ignite.internal.processors.hadoop.HadoopSetup

#
# Start utility.
#
. "${SCRIPTS_HOME}/ignite.sh" $@
