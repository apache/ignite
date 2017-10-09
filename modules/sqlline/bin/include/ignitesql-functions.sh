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
    echo "Usage: $self_name -ch=<ip_address> <options>"
    echo
    echo "Mandatory parameter:"
    echo "-ch= | --connectionHost= : Host to connect. Make sure an Ignite node is running on that host."
    echo
    echo "Non mandatory options:"
    echo "-p= | --port= : Port to connect."
    echo "-s= | --schema= : Schema."
    echo "-dj= | --distributedJoins= : Distributed joins flag."
    echo "-ej= | --enforceJoinOrder= : Enforce join order flag."
    echo "-c= | --collocated= : Collocated flag."
    echo "-r= | --replicatedOnly= : Replicated only flag."
    echo "-ac= | --autoCloseServerCursor= : Auto close server cursor flag."
    echo "-ssb= | --socketSendBuffer= : Socket send buffer size."
    echo "-srb= | --socketReceiveBuffer= : Socket receive buffer size."
    echo "-tnd= |--tcpNoDelay= : TCP no delay flag."
    echo "-l= |--lazy= : Lazy flag."
    echo
    echo "More information about these options:"
    echo "https://apacheignite-sql.readme.io/docs/jdbc-driver"
    echo
    echo "Example:"
    echo "ignitesql.sh -ch=127.0.0.1 -p=10800 -s=MySchema -dj=true -ej=true -ssb=0"
}

function edit_params()
{
    if [[ $PARAMS != "" ]]; then
        PARAM_DELIMITER="&"
    else
        PARAM_DELIMITER="?"
    fi

    if [ $1 != "" ]; then
         PARAMS="${PARAMS}${PARAM_DELIMITER}$2=$1"
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
            exit 0;
        ;;
           # Host to connect.
           -ch=*|--connectionHost=*)
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

    if [[ $HOST == "" ]]; then
        echo "Error. You need to specify host to connect:"
        echo "ignitesql.sh -ch=127.0.0.1"
        echo "Use -h or --help to read help."
        exit 1
    fi
}