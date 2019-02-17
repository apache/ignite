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
# Usage: ./update-versions 2.6.0
#

if [ $# -eq 0 ]
  then
    echo "Version not specified"
    exit 1
fi

echo Updating Java versions to $1 with Maven...
mvn versions:set -DnewVersion=$1 -Pall-java,all-scala,all-other -DgenerateBackupPoms=false -DgroupId=* -DartifactId=* -DoldVersion=* -DprocessDependencies=false

echo Updating .NET & C++ versions to $1 with Maven...
mvn validate -P update-versions -D new.ignite.version=$1
