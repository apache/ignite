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

if [ -z "$IGNITE_HOME" ]; then
    echo "Ignite source folder is not found or IGNITE_HOME environment variable is not valid."

    exit 1
fi

WORK_DIR=`cd "$(dirname "$0")"; pwd`

BUILD_DIR="$WORK_DIR/build"

IGNITE_WEB_CONSOLE_BACKEND_DIR="$IGNITE_HOME/modules/web-console/backend"
DOCKER_IMAGE_NAME="ignite/web-console-backend"

echo "Receiving version..."
VERSION=`cd $IGNITE_HOME && mvn org.apache.maven.plugins:maven-help-plugin:evaluate -Dexpression=project.version| grep -Ev '(^\[|Download\w+:)'`
RELEASE_VERSION=${VERSION%-SNAPSHOT}

echo "Building $DOCKER_IMAGE_NAME:$RELEASE_VERSION"
echo "Step 1. Prepare build temp paths."
cd $WORK_DIR
rm -Rf $BUILD_DIR
docker rmi -f $DOCKER_IMAGE_NAME:$RELEASE_VERSION

echo "Step 2. Build ignite web agent."
cd $IGNITE_HOME
mvn versions:set -DnewVersion=$RELEASE_VERSION -DgenerateBackupPoms=false -Pweb-console -DartifactId='*'
mvn clean package -pl :ignite-web-agent -am -P web-console -DskipTests=true
mvn versions:set -DnewVersion=$VERSION -DgenerateBackupPoms=false -Pweb-console -DartifactId='*'

echo "Step 3. Copy sources."
cd $WORK_DIR
cp -r $IGNITE_WEB_CONSOLE_BACKEND_DIR/. $BUILD_DIR
cp $IGNITE_HOME/modules/web-console/web-agent/target/ignite-web-agent*.zip $BUILD_DIR/agent_dists/.

echo "Step 4. Build docker image."
docker build -f=./Dockerfile -t $DOCKER_IMAGE_NAME:$RELEASE_VERSION .

echo "Step 5. Cleanup."
rm -Rf $BUILD_DIR
