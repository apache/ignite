#!/bin/sh
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

TESTS_ROOT=$(readlink -m $( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd ))
TESTS_CLASSPATH="$TESTS_ROOT/lib/*:$TESTS_ROOT/settings"

. $TESTS_ROOT/jvm-opt.sh $@

java $JVM_OPTS -cp "$TESTS_CLASSPATH" "org.apache.ignite.tests.IgnitePersistentStoreLoadTest"

if [ $? -ne 0 ]; then
    echo
    echo "--------------------------------------------------------------------------------"
    echo "[ERROR] Tests execution failed"
    echo "--------------------------------------------------------------------------------"
    echo
    exit 1
fi

echo
echo "--------------------------------------------------------------------------------"
echo "[INFO] Tests execution succeed"
echo "--------------------------------------------------------------------------------"
echo
