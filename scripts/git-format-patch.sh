#!/bin/bash
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
# Git patch-file maker.
#

#
# Imports and init
#
if [ -z ${IGNITE_HOME} ] # Script can be called from not IGNITE_HOME if IGNITE_HOME was set.
    then IGNITE_HOME=$PWD
fi

. ${IGNITE_HOME}/scripts/git-patch-prop.sh # Import properties.
. ${IGNITE_HOME}/scripts/git-patch-functions.sh # Import patch functions.

if [ -f ${IGNITE_HOME}/scripts/git-patch-prop-local.sh ] # Whether a local user properties file exists.
    then . ${IGNITE_HOME}/scripts/git-patch-prop-local.sh # Import user properties (it will rewrite global properties).
fi

CURRENT_BRANCH=$( determineCurrentBranch ${IGNITE_HOME} )

echo 'Usage: scripts/git-format-patch.sh.'
echo "It should be called from IGNITE_HOME directory."
echo "Patch will be created at PATCHES_HOME between Master branch (IGNITE_DEFAULT_BRANCH) and Current branch. "
echo
echo "IGNITE_HOME: ${IGNITE_HOME}"
echo "PATCHES_HOME: ${PATCHES_HOME}"
echo "Master branch: ${IGNITE_DEFAULT_BRANCH}"
echo "Current branch: ${CURRENT_BRANCH}"
echo

#
# Main script logic.
#

requireCleanWorkTree ${IGNITE_HOME}

formatPatch ${IGNITE_HOME} ${IGNITE_DEFAULT_BRANCH} ${CURRENT_BRANCH} _ignite.patch