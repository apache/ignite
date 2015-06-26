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

if [ -z $GIT_REPO ]; then
  echo Users git repo is not provided.

  exit 0
fi

git clone $GIT_REPO user-repo

cd user-repo

if [ ! -z $GIT_BRANCH ]; then
  git checkout $GIT_BRANCH
fi

if [ ! -z "$BUILD_CMD" ]; then
  echo "Starting to execute build command: $BUILD_CMD"

  eval "$BUILD_CMD"
else
  mvn clean package
fi