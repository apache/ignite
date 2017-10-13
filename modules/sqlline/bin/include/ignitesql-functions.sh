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
    echo "Usage: ${SELF_NAME} host[:port] [options]"
    echo
    echo "If port is omitted default port 10800 will be used."
    echo
    echo "Options:"
    echo "    -h  |  --help                       Help."
    echo "    --schema <schema>                   Schema name; defaults to PUBLIC."
    echo "    --distributedJoins                  Enable distributed joins."
    echo "    --lazy                              Execute queries in lazy mode."
    echo "    --collocated                        Collocated flag."
    echo "    --replicatedOnly                    Replicated only flag"
    echo "    --enforceJoinOrder                  Enforce join order."
    echo "    --socketSendBuffer <buf_size>       Socket send buffer size in bytes."
    echo "    --socketReceiveBuffer <buf_size>    Socket receive buffer size in bytes."
    echo
    echo "Examples: ${SELF_NAME} myHost --schema mySchema --distributedJoins"
    echo "          ${SELF_NAME} localhost --schema mySchema --collocated"
    echo "          ${SELF_NAME} 127.0.0.1:10800 --schema mySchema --replicatedOnly"
    echo
    echo "For more information see https://apacheignite-sql.readme.io/docs/jdbc-driver"

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
        print_help
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
        --schema)
            SCHEMA=$2
            if [[ $SCHEMA != "" ]]; then
                SCHEMA_DELIMITER="/";
            fi
            shift
            shift
        ;;
        # Distributed joins flag.
        --distributedJoins)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "distributedJoins" $2
                shift
                shift
            else
                edit_params "distributedJoins" "true"
                shift
            fi
        ;;
        # Enforce join order flag.
        --enforceJoinOrder)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "enforceJoinOrder" $2
                shift
                shift
            else
                edit_params "enforceJoinOrder" "true"
                shift
            fi
        ;;
        # Collocated flag.
        --collocated)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "collocated" $2
                shift
                shift
            else
                edit_params "collocated" "true"
                shift
            fi
        ;;
        # Replicated only flag.
        --replicatedOnly)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "replicatedOnly" $2
                shift
                shift
            else
                edit_params "replicatedOnly" "true"
                shift
            fi
        ;;
        # Auto close server cursor flag.
        --autoCloseServerCursor)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "autoCloseServerCursor" $2
                shift
                shift
            else
                edit_params "autoCloseServerCursor" "true"
                shift
            fi
        ;;
        # Socket send buffer.
        --socketSendBuffer)
            edit_params "socketSendBuffer" $2
            shift
            shift
        ;;
        # Socket receive buffer.
        --socketReceiveBuffer)
            edit_params "socketReceiveBuffer" $2
            shift
            shift
        ;;
        # TCP no delay flag.
        --tcpNoDelay)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
                edit_params "tcpNoDelay" $2
                shift
                shift
            else
                edit_params "tcpNoDelay" "true"
                shift
            fi
        ;;
        # Lazy flag.
        --lazy)
            if [[ $2 != -* && $2 != "" ]]; then
                check_boolean $2 $key
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

function check_boolean()
{
    if [[ $1 != "" ]]; then
        if [[ $1 != true && $1 != false ]]; then
            echo "Error: $1 is not a valid value for $2. Value should be 'true' or 'false'."
            echo "If you use $2 with no value $2 will be set to 'true'."
            echo "Use ${SELF_NAME} -h or --help to read help."
            exit 1
        fi
    fi
}