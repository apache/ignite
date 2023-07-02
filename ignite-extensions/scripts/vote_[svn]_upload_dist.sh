#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

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

SERVER_URL="https://dist.apache.org/repos/dist/dev/ignite/ignite-extensions"

chmod +x release.properties
. ./release.properties

echo "============================================================================="
echo "The RC: ${EXTENSION_RC_TAG}"

# Uncomment subsequent line in case you want to remove incorrectly prepared RC
#svn rm -m "Removing redundant Release" ${SERVER_URL}/${EXTENSION_RC_TAG} || true

svn import svn/vote ${SERVER_URL}/${EXTENSION_RC_TAG} -m "A new RC ${EXTENSION_RC_TAG} added: Binaries and Sources"

### Output result and notes ###
echo
echo "============================================================================="
echo "Artifacts should be moved to RC repository"
echo "Please check results at:"
echo "${SERVER_URL}/${EXTENSION_RC_TAG}"