#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

#################################################################################
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
#################################################################################
#                                  README                                       #
#################################################################################
#
# This script is used to build an extension locally on developers environment and
# upload everything to the Maven Central Staging repository and distribution archive.
#
# Run script from the Apache Ignite Extensions root directory.
# Usage: ./scripts/extension-deploy.sh modules/zookeeper-ip-finder-ext/
#
# PREREQUISITES:
# - copy `settings.xml` template from scripts directory
# - `apache.releases.https` username/password pair exists
# - pgp signature configured
# - export GPG_TTY=$(tty)
#
# SCRIPT EXECUTION:
#
# The following conditions must be met in order to complete the deploy successfully:
# - git branch `release/ignite-zookeeper-ip-finder-ext-1.0.0` created;
# - there is no SNAPSHOT versions in the release branch (dependencies and extension version);
# - the RC tag `ignite-zookeeper-ip-finder-ext-1.0.0-rc1` is added to the last commit;
# - there is no uncommitted changes in the release branch;
#
#################################################################################

function _logger () {
  echo -e "$@\r" | tee -a $log
}

if [ $# -eq 0 ]
  then
    echo "Ignite Extension directory is not specified."
    exit 1
fi

GIT_HOME="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
SCRIPTS_HOME="${GIT_HOME}/scripts/"

### Import patch functions. ###
. ${SCRIPTS_HOME}/git-patch-functions.sh

server_id="apache.releases.https"
dist_url="https://dist.apache.org/repos/dist/dev/ignite/ignite-extensions/"
now=$(date +'%H%M%S')
dir=$1
module_name="ignite-$(sed 's/\/$//' <<< $1 |  cut -d '/' -f2)"

log=$(pwd)"/log_${module_name}_${now}.tmp"
touch ${log}

_logger "============================================================================="
_logger "Extension Module Name:    ${module_name}"

cd ${dir}

### Get version from pom.xml with respect to the Maven. ###
ext_ver=$(mvn help:evaluate -D expression=project.version -q -DforceStdout)
ignite_ver=$(mvn help:evaluate -D expression=ignite.version -q -DforceStdout)

_logger "Extension Version:        ${ext_ver}"
_logger "Extension Ignite Version: ${ignite_ver}"

### Get the RC tag associated with the last commit in the current branch. ###
#rc_tag="${module_name}-${ext_ver}-rc1"
rc_tag=$(git describe --tags --exact-match --abbrev=0)

if [[ rc_tag =~ "${module_name}-${ext_ver}-rc"* ]]; then
  _logger "ERROR: The RC tag must have the following format: ignite-zookeeper-if-finder-ext-1.0.0-rc1"
  _logger "ERROR: Given tag: ${rc_tag}"

  exit 1;
fi

_logger "Extension RC tag:         ${rc_tag}"

requireCleanWorkTree ${GIT_HOME}

### Build the Extension ###
_logger "============================================================================="
_logger "Start Maven Build ..."

cd ${dir}

### The mvn must be started from the module root directory to collect assembly sources.
mvn clean deploy -DskipTests -Pextension-release -amd | tee -a ${log}

while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *ERROR* ]]; then
        _logger "ERROR: building. Please check log file: ${log}."

        exit 1;
    fi
done < ${log}

cd ${dir}

### Prepare sources and binary packages. ###
list=$(find $(pwd) -regex '.*\.zip' -o -regex '.*\.zip\.asc' -o -regex '.*\.zip\.sha512')
svn_dir=$(pwd)"/target/svn"
mkdir ${svn_dir}

_logger
_logger "============================================================================="
_logger "Copy assemblies (zip, asc, sha512) to the temporary svn directory: ${svn_dir}"

for file in $list
do
    _logger "Copying ${file}"

    cp ${file} ${svn_dir}
done

_logger
_logger "============================================================================="
_logger "Uploading RC to Apache dist: ${rc_tag}"

# Uncomment subsequent line in case you want to remove incorrectly prepared RC.
#svn rm -m "Removing redundant Release" https://dist.apache.org/repos/dist/dev/ignite/ignite-extensions/${rc_tag} || true
svn import ${svn_dir} ${dist_url}${rc_tag} -m "New RC ${rc_tag}: Sources and Binaries"


### Output result and notes ###
_logger
_logger "============================================================================="
_logger "Artifacts should be moved to RC repository"
_logger "Please check results at: "
_logger " - binaries: https://dist.apache.org/repos/dist/dev/ignite/ignite-extensions/${rc_tag}"
