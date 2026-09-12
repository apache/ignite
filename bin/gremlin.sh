#!/bin/bash
#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -e
set -u

USER_DIR=`pwd`

cd $(dirname $0)
DIR=`pwd`

SCRIPT_NAME=`basename $0`
SOURCE="${SCRIPT_NAME}"
while [ -h "${SCRIPT_NAME}" ]; do
  SOURCE="$(readlink "${SCRIPT_NAME}")"
  DIR="$( cd -P "$( dirname "${SOURCE}" )" && pwd )"
  cd ${DIR}
done
BIN="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Set $CFG to $BIN/../conf
cd -P $BIN/../config
CFG=$(pwd)
# Set $LIB to $BIN/../lib
cd -P $BIN/../libs
LIB=$(pwd)
# Set $LIB to $BIN/../ext
cd -P $BIN/../exts
EXT=$(pwd)


cd ..
SYSTEM_EXT_DIR="`pwd`/exts"
IGNITE_HOME=`pwd`
echo IGNITE_HOME=$IGNITE_HOME

JAVA_OPTIONS=${JAVA_OPTIONS:-}

#
# Set IGNITE_LIBS.
#
. "${IGNITE_HOME}"/bin/include/setenv.sh


CP=${SYSTEM_EXT_DIR}/*:${LIB}/*:${IGNITE_LIBS}

export CLASSPATH="${CLASSPATH:-}:$CP"

# Find Java
if [ -z "${JAVA_HOME:-}" ]; then
    JAVA="java -server"
else
    JAVA="$JAVA_HOME/bin/java -server"
fi

# Script debugging is disabled by default, but can be enabled with -l
# TRACE or -l DEBUG or enabled by exporting
# SCRIPT_DEBUG=nonemptystring to gremlin.sh's environment
if [ -z "${SCRIPT_DEBUG:-}" ]; then
    SCRIPT_DEBUG=
fi

# Process options
CFG=""
MAIN_CLASS=org.apache.tinkerpop.gremlin.console.Console
while getopts ":lv" opt; do
    case "$opt" in
    l) eval GREMLIN_LOG_LEVEL=\$$OPTIND
       OPTIND="$(( $OPTIND + 1 ))"
       if [ "$GREMLIN_LOG_LEVEL" = "TRACE" -o \
            "$GREMLIN_LOG_LEVEL" = "DEBUG" ]; then
	   SCRIPT_DEBUG=y
       fi
       ;;
    server) MAIN_CLASS=org.apache.tinkerpop.gremlin.server.GremlinServer
			CFG=config\gremlin-server\gremlin-server-ignite.yaml
			CP="${IGNITE_LIBS}"

    esac
done

if [ -z "${HADOOP_GREMLIN_LIBS:-}" ]; then
    export HADOOP_GREMLIN_LIBS="$LIB"
fi

JAVA_OPTIONS="${JAVA_OPTIONS} -Duser.working_dir=${USER_DIR} -Dtinkerpop.ext=${USER_EXT_DIR:-${SYSTEM_EXT_DIR}} -Dlog4j.configurationFile=file:config/log4j2-console.xml -cp $CP "
JAVA_OPTIONS=$(awk -v RS=' ' '!/^$/ {if (!x[$0]++) print}' <<< "${JAVA_OPTIONS}" | grep -v '^$' | paste -sd ' ' -)

if [ -n "$SCRIPT_DEBUG" ]; then
    # in debug mode enable debugging of :install command
    JAVA_OPTIONS="${JAVA_OPTIONS} -Divy.message.logger.level=4 -Dgroovy.grape.report.downloads=true"
    echo "CLASSPATH: $CLASSPATH"
    set -x
fi

# Start the JVM, execute the application, and return its exit code
exec $JAVA $JAVA_OPTIONS $MAIN_CLASS $CFG "$@"

