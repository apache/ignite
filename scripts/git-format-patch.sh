#!/bin/bash
#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
#

#
# Git patch-file maker.
#
echo 'Usage: scripts/git-format-patch.sh [-ih|--ignitehome <path>] [-idb|--ignitedefbranch <branch-name>] [-ph|--patchhome <path>]'
echo 'It is a script to create patch between Current branch (branch with changes) and Default branch. The script is safe and does not break or lose your changes.'
echo "It should be called from IGNITE_HOME directory."
echo "Patch will be created at PATCHES_HOME (= IGNITE_HOME, by default) between Default branch (IGNITE_DEFAULT_BRANCH) and Current branch."
echo "Note: you can use ${IGNITE_HOME}/scripts/git-patch-prop-local.sh to set your own local properties (to rewrite settings at git-patch-prop-local.sh). "
echo 'Examples:'
echo '- Basic (with all defaults and properties from git-patch-prop.sh):  ./scripts/git-format-patch.sh'
echo '- Rewrite some defaults (see Usage):                                ./scripts/git-format-patch.sh -ph /home/user_name/patches'
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

IGNITE_CURRENT_BRANCH=$( determineCurrentBranch ${IGNITE_HOME} )

echo "IGNITE_HOME    : ${IGNITE_HOME}"
echo "Default branch : ${IGNITE_DEFAULT_BRANCH}"
echo "Current branch : ${IGNITE_CURRENT_BRANCH}"
echo
echo "PATCHES_HOME   : ${PATCHES_HOME}"
echo

#
# Main script logic.
#

requireCleanWorkTree ${IGNITE_HOME}

formatPatch ${IGNITE_HOME} ${IGNITE_DEFAULT_BRANCH} ${IGNITE_CURRENT_BRANCH} .patch
