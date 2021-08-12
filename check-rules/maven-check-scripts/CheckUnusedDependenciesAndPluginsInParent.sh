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
POMS=$(find ${ROOT} -name pom.xml | grep -v parent)
for xpath in "project/dependencyManagement/dependencies/dependency/artifactId/text()" \
             "project/build/pluginManagement/plugins/plugin/artifactId/text()"; do
	xpath -e "${xpath}" ${ROOT}/parent/pom.xml 2>&1 | \
      grep -vE '(NODE|Found)' | \
      while read -r declaration; do
        FOUND=false
        for pom in ${POMS}; do
            if grep -E "<artifactId>${declaration}</artifactId>" "${pom}" 2>&1 1>/dev/null; then
            	FOUND=true
                continue 2
            fi
        done
        for parent_xpath in "project/build/plugins" \
                            "project/dependencies"; do
            if xpath -e "${parent_xpath}" ${ROOT}/parent/pom.xml 2>&1 | \
              grep -E "<" | \
              grep -E "<artifactId>${declaration}</artifactId>" 2>&1 1>/dev/null; then
              	FOUND=true
                continue 2
            fi
        done
        if [ "${FOUND}" == "false" ]; then
            echo "${declaration}" >> unused-properties.txt
            echo "[ERROR] Found unused declaration: ${declaration}"
        fi
    done
done
