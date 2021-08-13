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
xpath -q -e "project/modules/module/text()" ${ROOT}/pom.xml > current-list
cat current-list | sort -h > sorted-list
DIFF="$(diff current-list sorted-list || true)"
if [ "${DIFF}" != "" ]; then
	echo "[ERROR] Modules are in unsorted order:"
    echo "${DIFF}"
fi
