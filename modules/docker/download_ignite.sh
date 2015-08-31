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

function download {
  wget -O ignite.zip $1

  unzip ignite.zip -d ignite

  rm ignite.zip

  exit 0
}

if [ ! -z $IGNITE_URL ]; then
  download $IGNITE_URL
fi

if [ ! -z $IGNITE_VERSION ]; then
  if [[ $IGNITE_VERSION  =~ [0-9]*\.[0-9]*\.0 ]]; then
    download http://apache-mirror.rbc.ru/pub/apache/ignite/${IGNITE_VERSION}/apache-ignite-fabric-${IGNITE_VERSION}-bin.zip
  else
    download http://www.gridgain.com/media/gridgain-community-fabric-${IGNITE_VERSION}.zip
  fi
fi

if [ -z $IGNITE_SOURCE ] || [ $IGNITE_SOURCE = "COMMUNITY" ]; then
  download http://tiny.cc/updater/download_community.php
fi

if [ $IGNITE_SOURCE = "APACHE" ]; then
  download http://tiny.cc/updater/download_ignite.php
fi

echo "Unsupported IGNITE_SOURCE type: ${IGNITE_SOURCE}"
