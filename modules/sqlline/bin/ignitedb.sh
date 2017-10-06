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
# Ignite database launcher.
#

self_name=$(basename $0)

function print_help()
{
    echo "Script for connecting to cluster via sqlline."
    echo "Usage: $self_name options..."
    echo
}

function edit_params()
{
    if [[ $params != "" ]]; then
        param_delimiter="&"
    else
        param_delimiter="?"
    fi

    if [ $1 != "" ]; then
         params="${params}${param_delimiter}$2=$1"
    fi
}

# Default host:
HOST="127.0.0.1"

for i in "$@"
do
   case $i in
        # Print help
        -h|--help)
            print_help
        ;;
        # Host to connect. By default 127.0.0.1.
        -h=*|--host=*)
            HOST="${i#*=}"
            shift # get value after "="
        ;;
        # Port to connect. By default 10800.
        -p=*|--port=*)
            PORT="${i#*=}"
            if [ $PORT != "" ]; then
                port_delimiter=":";
            fi
            shift # get value after "="
        ;;
        # Schema.
        -s=*|--schema=*)
            SCHEMA="${i#*=}"
            if [ $SCHEMA != "" ]; then
                schema_delimiter="/";
            fi
            shift # get value after "="
        ;;
        # Distributed joins flag.
        -dj=*|--distributedJoins=*)
            edit_params "${i#*=}" "distributedJoins"
            shift # get value after "="
        ;;
        # Enforce join order flag.
        -ej=*|--enforceJoinOrder=*)
            edit_params "${i#*=}" "enforceJoinOrder"
            shift # get value after "="
        ;;
        # Collocated flag.
        -c=*|--collocated=*)
            edit_params "${i#*=}" "collocated"
            shift # get value after "="
        ;;
        # Replicated only flag.
        -r=*|--replicatedOnly=*)
            edit_params "${i#*=}" "replicatedOnly"
            shift # get value after "="
        ;;
        # Auto close server cursor flag.
        -ac=*|--autoCloseServerCursor=*)
            edit_params "${i#*=}" "autoCloseServerCursor"
            shift # get value after "="
        ;;
        # Socket send buffer.
        -ssb=*|--socketSendBuffer=*)
            edit_params "${i#*=}" "socketSendBuffer"
            shift # get value after "="
        ;;
        # Socket receive buffer.
        -srb=*|--socketReceiveBuffer=*)
            edit_params "${i#*=}" "socketReceiveBuffer"
            shift # get value after "="
        ;;
        # TCP no delay flag.
        -tnd=*|--tcpNoDelay=*)
            edit_params "${i#*=}" "tcpNoDelay"
            shift # get value after "="
        ;;
        # Lazy flag.
        -l=*|--lazy=*)
            edit_params "${i#*=}" "lazy"
            shift # get value after "="
        ;;

        *)
        ;;
    esac
done

jdbclink="jdbc:ignite:thin://${HOST}${port_delimiter}${PORT}${schema_delimiter}${SCHEMA}${params}"

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ];
    then IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else IGNITE_HOME_TMP=${IGNITE_HOME};
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
# Set IGNITE_LIBS.
#
. "${SCRIPTS_HOME}"/include/setenv.sh

CP="${IGNITE_LIBS}"

CP=${CP}:${IGNITE_HOME_TMP}/bin/include/sqlline/*

java -cp ${CP} sqlline.SqlLine -d org.apache.ignite.IgniteJdbcThinDriver --color=true --verbose=true --showWarnings=true --showNestedErrs=true -u ${jdbclink}
