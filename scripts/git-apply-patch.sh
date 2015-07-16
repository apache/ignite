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
# Git patch-file applier.
#
echo 'Usage: scripts/git-apply-patch.sh <ignite-task> [-ih|--ignitehome <path>] [-idb|--ignitedefbranch <branch-name>] [-ph|--patchhome <path>]'
echo "It should be called from IGNITE_HOME directory."
echo "Patch will be applied to DEFAULT_BRANCH from PATCHES_HOME."
echo "Note: you can use ${IGNITE_HOME}/scripts/git-patch-prop-local.sh to set your own local properties (to rewrite settings at git-patch-prop-local.sh). "
echo

#
# Init home and import properties and functions.
#
if [ -z ${IGNITE_HOME} ] # Script can be called from not IGNITE_HOME if IGNITE_HOME was set.
    then IGNITE_HOME=$PWD
fi

. ${IGNITE_HOME}/scripts/git-patch-prop.sh # Import properties.
. ${IGNITE_HOME}/scripts/git-patch-functions.sh # Import patch functions.

if [ -f ${IGNITE_HOME}/scripts/git-patch-prop-local.sh ] # Whether a local user properties file exists.
    then . ${IGNITE_HOME}/scripts/git-patch-prop-local.sh # Import user properties (it will rewrite global properties).
fi

#
# Ignite task.
#
IGNITE_TASK=$1

#
# Read command line params.
#
while [[ $# > 1 ]]
do
    key="$1"

    case $key in
        -ih|--ignitehome)
        IGNITE_HOME="$2"
        shift
        ;;

        -idb|--ignitedefbranch)
        IGNITE_DEFAULT_BRANCH="$2"
        shift
        ;;

        -ph|--patchhome)
        PATCHES_HOME="$2"
        shift
        ;;
        *)

        echo "Unknown parameter: ${key}"
        ;;
    esac
    shift
done

echo "IGNITE_HOME    : ${IGNITE_HOME}"
echo "Default branch : ${IGNITE_DEFAULT_BRANCH}"
echo "Ignite task    : ${IGNITE_TASK}"
echo
echo "PATCHES_HOME   : ${PATCHES_HOME}"
echo

#
# Main script logic.
#

currentAndDefaultBranchesShouldBeEqual ${IGNITE_HOME} ${IGNITE_DEFAULT_BRANCH}

requireCleanWorkTree ${IGNITE_HOME}

IGNITE_PATCH_FILE=${PATCHES_HOME}/${IGNITE_DEFAULT_BRANCH}_${IGNITE_TASK}.patch

applyPatch ${IGNITE_HOME} ${IGNITE_DEFAULT_BRANCH} ${IGNITE_PATCH_FILE}
