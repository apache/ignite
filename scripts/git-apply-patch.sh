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
# Git patch-file applyer.
#

PATCHED_BRANCH=$1

if [ "${GG_HOME}" = "" ];
    then GG_HOME=$PWD
fi

. ${GG_HOME}/scripts/git-patch-prop.sh # Import properties.
. ${GG_HOME}/scripts/git-patch-prop-local.sh # Import user properties (it will rewrite global properties).

applyPatch () {
    GIT_HOME=$1
    DEFAULT_BRANCH=$2
    PATCH_FILE=$3

    cd ${GIT_HOME}

    git checkout ${DEFAULT_BRANCH}

#    git apply ${PATCH_FILE}
    git am ${PATCH_FILE}
}

IGNITE_PATCH_FILE=${PATCHES_HOME}/${IGNITE_DEFAULT_BRANCH}_${PATCHED_BRANCH}_ignite.patch
GG_PATCH_FILE=${PATCHES_HOME}/${GG_DEFAULT_BRANCH}_${PATCHED_BRANCH}_gg.patch

if [ -f ${IGNITE_PATCH_FILE} ] # Whether a patch-file exists
then
    applyPatch ${IGNITE_HOME} ${IGNITE_DEFAULT_BRANCH} ${IGNITE_PATCH_FILE}
fi

if [ -f ${GG_PATCH_FILE} ] # Whether a patch-file exists
then
    applyPatch ${GG_HOME} ${GG_DEFAULT_BRANCH} ${GG_PATCH_FILE}
fi
