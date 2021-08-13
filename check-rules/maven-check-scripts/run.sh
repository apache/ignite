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

cleanup() {
    rm -rf current-list \
           sorted-list \
           unused-properties.txt
}
trap cleanup EXIT SIGINT ERR

command -v xpath > /dev/null || {
    echo "xpath not found, exiting"
}

DIR__MAVEN_CHECK_SCRIPTS="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
for script in CheckDependencyAndPluginVersionsNotInParent.sh \
              CheckModulesInRootPomAreSorted.sh \
              CheckPropertiesNotInParent.sh \
              CheckUnusedDependenciesAndPluginsInParent.sh \
              CheckUnusedProperties.sh; do
    echo -n " * Executing ${script}... "
    bash ${DIR__MAVEN_CHECK_SCRIPTS}/${script} && \
        echo "Done" || {
            echo "[ERROR]"
            exit 1
        }
done
echo
echo "All checks finished successfully"
