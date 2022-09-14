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

#
# Run from the Apache Ignite sources root directory.
# Usage: ./scripts/update-versions [2.13.0] modules/zookeeper-ip-finder-ext/ 1.0.0
#

if [ $# -eq 0 ]
  then
    echo "Version not specified"
    exit 1
fi

git_root=$(pwd)
module_name="ignite-$(sed 's/\/$//' <<< $2 |  cut -d '/' -f2)"

cd parent-internal

echo "============================================================================="
echo "Updating Apache Ignite parent version to $1 with Maven..."
mvn versions:update-parent -DparentVersion=$1 -DgenerateBackupPoms=false

### Use changing version command from the extension directory.  ###
cd ${git_root}/$2

echo "============================================================================="
echo "Updating Extension ${module_name} version to $3 with Maven..."
mvn versions:set -DnewVersion=$3 -DgenerateBackupPoms=false -DgroupId=* -DartifactId=* -DoldVersion=* -DprocessDependencies=false
