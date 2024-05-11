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
# Git patch functions.
#

#
# Define functions.
#

#
# Formats patch. Create patch in one commit from user who run script and with default comment.
#
# Params:
# - Git home.
# - Default branch.
# - Patch with patch.
# - Suffix for created patch-file.
#
formatPatch () {
    GIT_HOME=$1
    DEFAULT_BRANCH=$2
    PATCHED_BRANCH=$3
    PATCH_SUFFIX=$4

    if [ ${IGNITE_CURRENT_BRANCH} = ${IGNITE_DEFAULT_BRANCH} ]
    then
        echo $0", ERROR:"
        echo "You are on Default branch. Please, checkout branch with changes."

        exit 1
    fi

    cd ${GIT_HOME}

    git checkout ${DEFAULT_BRANCH}

    DEF_BRANCH_REV="$(git rev-parse --short HEAD)"

    git checkout -b tmppatch

    # Merge to make only one commit.
    git merge --squash ${PATCHED_BRANCH}
    git commit -a -m "# ${PATCHED_BRANCH}"

    PATCH_FILE=${PATCHES_HOME}'/'${DEFAULT_BRANCH}_${DEF_BRANCH_REV}_${PATCHED_BRANCH}${PATCH_SUFFIX}

    git format-patch ${DEFAULT_BRANCH}  --stdout > ${PATCH_FILE}
    echo "Patch file created."

    git checkout ${PATCHED_BRANCH}

    git branch -D tmppatch # Delete tmp branch.

    echo
    echo "Patch created: ${PATCH_FILE}"
}

#
# Determines Current branch.
#
# Params:
# - Git home.
# Return - Current branch.
#
determineCurrentBranch () {
    GIT_HOME=$1

    cd ${GIT_HOME}

    CURRENT_BRANCH=`git rev-parse --abbrev-ref HEAD`

    echo "$CURRENT_BRANCH"
}

#
# Checks that given git repository has clean work tree (there is no uncommited changes).
# Exit with code 1 in error case.
#
# Params:
# - Git home.
#
requireCleanWorkTree () {
    cd $1 # At git home.

    # Update the index
    git update-index -q --ignore-submodules --refresh
    err=0

    # Disallow unstaged changes in the working tree
    if ! git diff-files --quiet --ignore-submodules --
    then
        echo $0", ERROR:"
        echo >&2 "You have unstaged changes."
        git diff-files --name-status -r --ignore-submodules -- >&2
        err=1
    fi

    # Disallow uncommitted changes in the index
    if ! git diff-index --cached --quiet HEAD --ignore-submodules --
    then
        echo $0", ERROR:"
        echo >&2 "Your index contains uncommitted changes."
        git diff-index --cached --name-status -r --ignore-submodules HEAD -- >&2
        err=1
    fi

    if [ $err = 1 ]
    then
        echo >&2 "Please commit or stash them."
        exit 1
    fi
}


