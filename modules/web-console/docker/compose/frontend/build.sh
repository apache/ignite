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

SOURCE_DIR=$WORK_DIR/src
BUILD_DIR=$WORK_DIR/build

DOCKER_BUILD_CONTAINER=web-console-frontend-builder
DOCKER_BUILD_IMAGE_NAME=ignite/$DOCKER_BUILD_CONTAINER
DOCKER_IMAGE_NAME=ignite/web-console-frontend

echo "Receiving version..."
VERSION=`cd $IGNITE_HOME && mvn org.apache.maven.plugins:maven-help-plugin:evaluate -Dexpression=project.version| grep -Ev '(^\[|Download\w+:)'`
RELEASE_VERSION=${VERSION%-SNAPSHOT}

echo "Building $DOCKER_IMAGE_NAME:$RELEASE_VERSION"
echo "Step 1. Build frontend SPA"
cd $WORK_DIR

rm -Rf $SOURCE_DIR
rm -Rf $BUILD_DIR
mkdir -p $SOURCE_DIR
mkdir -p $BUILD_DIR

cp -r $IGNITE_HOME/modules/web-console/frontend/. $SOURCE_DIR

docker build -f=./DockerfileBuild -t $DOCKER_BUILD_IMAGE_NAME:latest .
docker run -it -v $BUILD_DIR:/opt/web-console-frontend/build --name $DOCKER_BUILD_CONTAINER $DOCKER_BUILD_IMAGE_NAME

echo "Step 2. Build NGINX container with SPA and proxy configuration"
docker build -f=./Dockerfile -t $DOCKER_IMAGE_NAME:$RELEASE_VERSION .

echo "Step 3. Cleanup"
docker rm -f $DOCKER_BUILD_CONTAINER
docker rmi -f $DOCKER_BUILD_IMAGE_NAME
rm -r $SOURCE_DIR
rm -r $BUILD_DIR
