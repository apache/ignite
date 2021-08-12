#!/usr/bin/env bash
set -o nounset; set -o errexit; set -o pipefail; set -o errtrace; set -o functrace

#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )/../.."
POMS=$(find ${ROOT} -name pom.xml)
xpath -e "project/properties/*" ${ROOT}/parent/pom.xml 2>&1 | \
  grep -E "^<.*\.version>" | \
  sed -r 's|<(.*)>.*<\/.*>|\1|' | \
  while read -r property; do
    FOUND=false
    for pom in ${POMS}; do
        if grep -qE "\\$.*${property}" "${pom}"; then
            FOUND=true
        fi
    done
    if [ "${FOUND}" == "false" ]; then
        echo "${property}" > unused-properties.txt
        echo "[ERROR] Found unused property: ${property}"
    fi
done
