#!/usr/bin/env bash

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

set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace

MYSELF=$(which "${0}" 2>/dev/null)
[ $? -gt 0 -a -f "${0}" ] && MYSELF="./${0}"
java=java
if test -n "${JAVA_HOME:-}"; then
    java="${JAVA_HOME}/bin/java"
fi
exec "${java}" ${java_args:-} -jar ${MYSELF} "$@"
exit 1
