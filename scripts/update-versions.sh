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
# Updates Ignite version in Java pom files, .NET AssemblyInfo files, C++ configure files.
# Run in Ignite sources root directory.
# Usage:
# update snapshot version: ./scripts/update-versions.sh 2.14.0-SNAPSHOT
# update release version: ./scripts/update-versions.sh 2.13.0
#

if [ $# -eq 0 ]
  then
    echo "Version not specified"
    exit 1
fi

SED_OPTION=(-i)

if [[ "$OSTYPE" == "darwin"* ]]; then
  SED_OPTION=(-i '')
fi

echo Updating versions to "$1"

# The ignite-checkstyle module has it's own Apache parent, so in has to be updated independently.
sed "${SED_OPTION[@]}" -e "s/<revision>\(.*\)</<revision>${1}</g" ./parent/pom.xml ./modules/checkstyle/pom.xml;

echo Updating sub-modules versions to "$1" and resouces during the build

mvn install -P all-java,all-scala,platforms,skip-docs -DskipTests
