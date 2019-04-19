#!/bin/bash
#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
# 
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Builds Java and .NET with Mono
#

# Fail on error.
set -e

# Build Java.
pushd .
cd ../../..
mvn clean package -DskipTests -Dmaven.javadoc.skip=true -Plgpl,-examples,-clean-libs,-release,-scala,-clientDocs
popd

# Build .NET.
nuget restore Apache.Ignite.sln
msbuild Apache.Ignite.sln /p:RunCodeAnalysis=false
