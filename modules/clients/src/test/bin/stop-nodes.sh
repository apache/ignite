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
