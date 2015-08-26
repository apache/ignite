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
# Pull request applier.
#
echo 'Usage: scripts/apply-pull-request.sh <pull-request-id>'
echo 'The script takes pull-request by given id and merges (with squash) all changes too master branch.'
echo "Argument 'pull-request-id' is mandatory."
echo

IGNITE_HOME="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";

. ${IGNITE_HOME}/scripts/git-patch-functions.sh # Import patch functions.

# Constants.
APACHE_GIT="https://git-wip-us.apache.org/repos/asf/ignite"
GITHUB_MIRROR="https://github.com/apache/ignite.git"

# Get paramethers.
PR_ID=$1

# Initial checks.
if [ "${PR_ID}" = "" ]; then
    echo $0", ERROR:"
    echo >&2 "You have to specify 'pull-request-id'."
    exit 1
fi

requireCleanWorkTree ${IGNITE_HOME}

CURRENT_BRANCH=`git rev-parse --abbrev-ref HEAD`

if [ "$CURRENT_BRANCH" != "master" ]; then
    echo $0", ERROR:"
    echo "You have to be on master branch."

    exit 1
fi

# Check that master is up-to-date.
APACHE_GIT_MASTER_BRANCH="apache-git-master-tmp"

git fetch ${APACHE_GIT} master:${APACHE_GIT_MASTER_BRANCH} &> /dev/null

LOCAL_MASTER_HASH=$(git rev-parse @)
REMOTE_MASTER_HASH=$(git rev-parse ${APACHE_GIT_MASTER_BRANCH})
BASE_HASH=$(git merge-base @ ${APACHE_GIT_MASTER_BRANCH})

git branch -D ${APACHE_GIT_MASTER_BRANCH} &> /dev/null

if [ $LOCAL_MASTER_HASH != $REMOTE_MASTER_HASH ]; then
    echo $0", ERROR:"

    if [ $LOCAL_MASTER_HASH = $BASE_HASH ]; then
        echo "Your local master branch is not up-to-date. You need to pull."
    elif [ $REMOTE_MASTER_HASH = $BASE_HASH ]; then
        echo "Your local master branch is ahead of master branch at Apache git. You need to push."
    else
        echo "Your local master and Apache git master branches diverged. You need to pull, merge and pull."
    fi

    exit 1
fi

echo "Local master is Up-to-date."
echo

# Checkout pull-request branch.
PR_BRANCH_NAME="pull-${PR_ID}-head"

git fetch ${GITHUB_MIRROR} pull/${PR_ID}/head:${PR_BRANCH_NAME} &> /dev/null
if test $? != 0; then
    echo $0", ERROR:"
    echo >&2 "There was not found pull request by ID = '${PR_ID}'."
    exit 1
fi

# Get author name number.
git checkout ${PR_BRANCH_NAME} &> /dev/null
if test $? != 0; then
    echo $0", ERROR:"
    echo >&2 "Failed to checkout '${PR_BRANCH_NAME}' branch (the branch not found or already exists)."
    exit 1
fi

AUTHOR="$(git --no-pager show -s --format="%aN <%aE>" HEAD)"
ORIG_COMMENT="$(git log -1 --pretty=%B)"

echo "Author of pull-request: '$AUTHOR'."
echo

# Update local master.
git checkout master &> /dev/null

# Take changes.
git merge --squash ${PR_BRANCH_NAME} &> /dev/null
if test $? != 0; then
    git reset --hard &> /dev/null

    echo $0", ERROR:"
    echo >&2 "Could not merge the pull-request to master without conflicts. All local changes have been discarded. You're on master branch."
    exit 1
fi

echo "Original comment is"
echo "\"${ORIG_COMMENT}\""
echo "Press [ENTER] if you're agree with the comment or type your comment and press [ENTER]:"
read COMMENT
echo

if [ "${COMMENT}" == "" ]; then
    COMMENT=${ORIG_COMMENT}
fi

git commit --author "${AUTHOR}" -a -s -m "${COMMENT}" &> /dev/null

echo "Squash commit for pull request with id='${PR_ID}' has been added. The commit has been added with comment '${COMMENT}'."
echo "Now you can review changes of the last commit at master and push it to Ignite Apche git after."
echo "If you want to decline changes, you can remove the last commit from your repo by 'git reset --hard HEAD^'."
echo

# Clean-up.
git branch -D ${PR_BRANCH_NAME} &> /dev/null

echo 'Successfully completed.'
