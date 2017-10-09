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

function print_help()
{
    echo
    echo "Script for connecting to cluster via sqlline."
    echo "Usage: ${SELF_NAME} <ip_address:port> <options>"
    echo
    echo "In the first parameter you have to explicitly specify a host to connect."
    echo "Make sure an Ignite node is running on that host."
    echo "For example:"
    echo "${SELF_NAME} 127.0.0.1"
    echo "Optionally you can specify a port:"
    echo "${SELF_NAME} 127.0.0.1:10800"
    echo
    echo "Non mandatory options:"
    echo "-s   | --schema : Schema."
    echo "-dj  | --distributedJoins : Distributed joins flag."
    echo "-ej  | --enforceJoinOrder : Enforce join order flag."
    echo "-c   | --collocated : Collocated flag."
    echo "-r   | --replicatedOnly : Replicated only flag."
    echo "-ac  | --autoCloseServerCursor : Auto close server cursor flag."
    echo "-ssb | --socketSendBuffer : Socket send buffer size."
    echo "-srb | --socketReceiveBuffer : Socket receive buffer size."
    echo "-tnd | --tcpNoDelay : TCP no delay flag."
    echo "-l   | --lazy : Lazy flag."
    echo
    echo "More information about these options:"
    echo "https://apacheignite-sql.readme.io/docs/jdbc-driver"
    echo
    echo "Example:"
    echo "ignitesql.sh 127.0.0.1 -s MySchema -dj true -ej -ssb 0"
}

function edit_params()
{
    if [[ $PARAMS != "" ]]; then
        PARAM_DELIMITER="&"
    else
        PARAM_DELIMITER="?"
    fi

    if [[ $# -ne 1 ]]; then
        PARAMS="${PARAMS}${PARAM_DELIMITER}$1=$2"
    else
        PARAMS="${PARAMS}${PARAM_DELIMITER}$1=true"
    fi
}

function parse_arguments()
{
    if [[ $# -eq 0 ]]; then
        echo "Error. You need to specify host to connect:"
        echo "ignitesql.sh 127.0.0.1"
        echo "Use -h or --help to read help."
        exit 1
    fi

    if [[ $1 == "-h" || $1 == "--help" ]]; then
        print_help
        exit 0
    else
        HOST_AND_PORT=$1
        shift
    fi

    while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        # Help.
        -h|--help)
            print_help
            exit 0
        ;;
        # Schema.
        -s|--schema)
            SCHEMA=$2
            if [[ $SCHEMA != "" ]]; then
                SCHEMA_DELIMITER="/";
            fi
            shift
            shift
        ;;
        # Distributed joins flag.
        -dj|--distributedJoins)
            if [[ $2 != -* ]]; then
                edit_params "distributedJoins" $2
                shift
                shift
            else
                edit_params "distributedJoins" "true"
                shift
            fi
        ;;
        # Enforce join order flag.
        -ej|--enforceJoinOrder)
            if [[ $2 != -* ]]; then
                edit_params "enforceJoinOrder" $2
                shift
                shift
            else
                edit_params "enforceJoinOrder" "true"
                shift
            fi
        ;;
        # Collocated flag.
        -c|--collocated)
            if [[ $2 != -* ]]; then
                edit_params "collocated" $2
                shift
                shift
            else
                edit_params "collocated" "true"
                shift
            fi
        ;;
        # Replicated only flag.
        -r|--replicatedOnly)
            if [[ $2 != -* ]]; then
                edit_params "replicatedOnly" $2
                shift
                shift
            else
                edit_params "replicatedOnly" "true"
                shift
            fi
        ;;
        # Auto close server cursor flag.
        -ac|--autoCloseServerCursor)
            if [[ $2 != -* ]]; then
                edit_params "autoCloseServerCursor" $2
                shift
                shift
            else
                edit_params "autoCloseServerCursor" "true"
                shift
            fi
        ;;
        # Socket send buffer.
        -ssb|--socketSendBuffer)
            edit_params "socketSendBuffer" $2
            shift
            shift
        ;;
        # Socket receive buffer.
        -srb|--socketReceiveBuffer)
            edit_params "socketReceiveBuffer" $2
            shift
            shift
        ;;
        # TCP no delay flag.
        -tnd|--tcpNoDelay)
            if [[ $2 != -* ]]; then
            edit_params "tcpNoDelay" $2
                shift
                shift
            else
                edit_params "tcpNoDelay" "true"
                shift
            fi
        ;;
        # Lazy flag.
        -l|--lazy)
            if [[ $2 != -* ]]; then
                edit_params "lazy" $2
                shift
                shift
            else
                edit_params "lazy" "true"
                shift
            fi
        ;;
        # Unknown argument
        *)
            echo "Error: unknown argument: ${key}."
            print_help
            exit 1;
        ;;
    esac
done
}