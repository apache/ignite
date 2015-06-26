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

if [ ! -z "$GIT_REPO" ]; then
  if [ -z "$LIB_PATTERN" ]; then
    find user-repo/ -regextype posix-extended -regex "user-repo/target/.*(jar|zip)$" -exec cp {} ignite/*/libs \;
  else
    find user-repo/ -regextype posix-extended -regex "$LIB_PATTERN" -exec cp {} ignite/*/libs \;
  fi
fi

if [ -z "$OPTION_LIBS" ]; then
  OPTION_LIBS="ignite-log4j"
fi

if [ ! -z "$OPTION_LIBS" ]; then
  IFS=, LIBS_LIST=("$OPTION_LIBS")

  for lib in ${LIBS_LIST[@]}; do
    cp -r ./ignite/*/libs/optional/"$lib"/* ./ignite/*/libs
  done
fi

# Try to download
if [ ! -z "$IGNITE_CONFIG" ]; then
  wget -O ignite-config.xml "$IGNITE_CONFIG" 2>/dev/null

  RETVAL=$?

  [ $RETVAL -eq 0 ] && IGNITE_CONFIG=ignite-config.xml

  [ $RETVAL -ne 0 ] && rm ignite-config.xml && echo "Failed download config: $IGNITE_CONFIG. Try to load config from classpath."
fi

if [ ! -z "$EXEC_CMD" ]; then
  echo "Starting to execute command: $EXEC_CMD"

  eval "$EXEC_CMD"

  exit 0
fi

if [ -z "$IGNITE_CONFIG" ]; then
  ignite/*/bin/ignite.sh
else
  ignite/*/bin/ignite.sh ignite-config.xml
fi