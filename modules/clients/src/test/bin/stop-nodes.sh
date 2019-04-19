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
# Grid command line loader.
#

echo Kill all grid nodes.

for X in `$JAVA_HOME/bin/jps | grep -i -G Grid.*CommandLineLoader | awk {'print $1'}`; do
    kill -9 $X
done

for X in `$JAVA_HOME/bin/jps | grep -i -G Grid.*CommandLineStartup | awk {'print $1'}`; do
    kill -9 $X
done

# Get a time to kill java processes.
sleep 1

echo "Rest java processes (ps ax | grep java):"
ps ax | grep java

# Force exit code 0.
echo "Done."
