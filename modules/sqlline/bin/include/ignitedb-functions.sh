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

function print_help()
{
    echo "Script for connecting to cluster via sqlline."
    echo "Usage: $self_name options..."
    echo
}

function edit_params()
{
    if [[ $PARAMS != "" ]]; then
        param_delimiter="&"
    else
        param_delimiter="?"
    fi

    if [ $1 != "" ]; then
         PARAMS="${PARAMS}${param_delimiter}$2=$1"
    fi
}

function parse_arguments()
{
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
                PORT_DELIMITER=":";
            fi
            shift # get value after "="
        ;;
        # Schema.
        -s=*|--schema=*)
            SCHEMA="${i#*=}"
            if [ $SCHEMA != "" ]; then
                SCHEMA_DELIMITER="/";
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

}