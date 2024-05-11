#!/usr/bin/env bash

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

chmod +x release.properties
. ./release.properties

function _logger () {
  echo -e "$@\r" | tee -a $log
}

server_url="https://repository.apache.org/service/local/staging/deploy/maven2"
server_id="apache.releases.https"

now=$(date +'%H%M%S')
log="vote_[mvn][pgp]_deploy_${now}.log"

_logger "============================================================================="
_logger "Preparing Maven Upload ${EXTENSION_RC_TAG}"

dir="./maven/org/apache/ignite"

main_file=$(find $dir -name "${EXTENSION_NAME}-${EXTENSION_VERSION}.jar")
pom=$(find $dir -name "${EXTENSION_NAME}-${EXTENSION_VERSION}.pom")
javadoc=$(find $dir -name "${EXTENSION_NAME}-${EXTENSION_VERSION}-javadoc.jar")
sources=$(find $dir -name "${EXTENSION_NAME}-${EXTENSION_VERSION}-sources.jar")
tests=$(find $dir -name "${EXTENSION_NAME}-${EXTENSION_VERSION}-tests.jar")

adds=""

if [[ $javadoc == *javadoc* ]]
then
	adds="${adds} -Djavadoc=${javadoc}"
fi

if [[ $sources == *sources* ]]
then
	adds="${adds} -Dsources=${sources}"
fi

if [[ $tests == *tests* ]]
then
	adds="${adds} -Dfiles=${tests} -Dtypes=jar -Dclassifiers=tests"
fi

if [[ ! -n $main_file && ! -n $features ]]
then
	main_file=$pom
	adds="-Dpackaging=pom"
fi

_logger "Directory: $dir"
_logger "File: $main_file"
_logger "Adds: $adds"

export GPG_TTY=$(tty)

mvn gpg:sign-and-deploy-file -Pgpg -Dfile=$main_file -Durl=$server_url -DrepositoryId=$server_id \
  -DretryFailedDeploymentCount=10 -DpomFile=$pom ${adds} | tee -a ${log}

result="Uploaded"

while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *ERROR* ]]
    then
        result="Uploading failed. Please check log file: ${log}."
    fi
done < ${log}

_logger ${result}

_logger " "
_logger "============================================================================="
_logger "Maven staging should be created"
_logger "Please check results at"
_logger "https://repository.apache.org/#stagingRepositories"
