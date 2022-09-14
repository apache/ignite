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

now=$(date +'%H%M%S')
log="vote_[pgp]_sign_binary_${now}.log"

### Sign artifacts ###
echo "============================================================================="
echo "Starting GPG Agent"
gpg-connect-agent /bye

list=$(find ./svn/vote -type f -name "*.zip")

for file in $list
do
    echo "Signing ${file}"
	  echo ${file} | tee -a ${log}
    GPG_AGENT_INFO=~/.gnupg/S.gpg-agent:0:1 gpg -ab ${file} | tee -a ${log}
done

result="Signed OK."

while IFS='' read -r line || [[ -n "${line}" ]]; do
    if [[ $line == *ERROR* ]]
    then
        result="Signing failed. Please check log file: ${log}."
    fi
done < ${log}

echo ${result}


#
# Output result and notes
#
echo " "
echo "==============================================="
echo "Artifacts should be signed"
echo "Please check results at ./svn/vote"
echo "Each file should have corresponding *.asc file"
echo
echo "NOTE: Package files are not signed because they"
echo "are meant to be stored in Bintray"