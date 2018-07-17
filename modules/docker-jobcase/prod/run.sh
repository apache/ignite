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

if [ ! -z "$OPTION_LIBS" ]; then
  IFS=, LIBS_LIST=("$OPTION_LIBS")

  for lib in ${LIBS_LIST[@]}; do
    cp -r $IGNITE_HOME/libs/optional/"$lib"/* \
        $IGNITE_HOME/libs/
  done
fi

if [ ! -z "$EXTERNAL_LIBS" ]; then
  IFS=, LIBS_LIST=("$EXTERNAL_LIBS")

  for lib in ${LIBS_LIST[@]}; do
    wget $lib -P $IGNITE_HOME/libs
  done
fi

# form a consistent ID from the ECS host's name and the cluster name
if [ -z "$IGNITE_CONSISTENT_ID" ]; then
    if [ ! -z "$IGNITE_CLUSTER_NAME" ]  && [ ! -z "$IGNITE_PERSISTENT_STORE" ]  &&  [ -f "$IGNITE_PERSISTENT_STORE/hostname" ]; then
        HOST_NAME=`cat $IGNITE_PERSISTENT_STORE/hostname`
        export IGNITE_CONSISTENT_ID=${IGNITE_CLUSTER_NAME}-${HOST_NAME}
    fi
fi

export JVM_XOPTS="$JVM_XOPTS $JVM_DEBUG_OPTS -Xloggc:${JOBCASE_LOGS}/jvm-gc.log"

if [ ! -z "${JVM_METASPACE_SIZE}" ]; then
    export JVM_XOPTS="$JVM_XOPTS -XX:MaxMetaspaceSize=${JVM_METASPACE_SIZE}"
fi

if [ ! -z "${JVM_HEAP_SIZE}" ]; then
    export JVM_XOPTS="$JVM_XOPTS -Xms${JVM_HEAP_SIZE} -Xmx${JVM_HEAP_SIZE}"
fi    



QUIET=""

if [ "$IGNITE_QUIET" = "false" ]; then
  QUIET="-v"
fi

if [ -z $CONFIG_URI ]; then
  $IGNITE_HOME/bin/ignite.sh $QUIET
else
  $IGNITE_HOME/bin/ignite.sh $QUIET $CONFIG_URI
fi
