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

#
# Start of Functions.
#

#
# Prints usage.
#
usage () {
    echo 'Usage: scripts/apply-pull-request.sh <pull-request-id> [-tb|--targetbranch <branch-name>]'
    echo 'The script takes pull-request by given id and merges (with squash) all changes to target branch (master by default).'
    echo "Argument 'pull-request-id' is mandatory."
    echo "Target branch can be overwritten by using [-tb|--targetbranch <branch-name>] argument paramethers."
}

#
# End of Functions.
#

if [ "${GIT_HOME}" = "" ]; then
    GIT_HOME="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
fi

cd ${GIT_HOME}

if [ "${SCRIPTS_HOME}" = "" ]; then
    SCRIPTS_HOME="${GIT_HOME}/scripts/"
fi

. ${SCRIPTS_HOME}/git-patch-functions.sh # Import patch functions.

PR_ID=$1

#
# Start reading of command line params.
#
if [ "${PR_ID}" = "" ]; then
    echo $0", ERROR:"
    echo >&2 "You have to specify 'pull-request-id'."
    echo
    usage
    exit 1
fi

if [ "${PR_ID}" = "-h" ]; then
    usage
    exit 0
fi

if [ "${PR_ID}" = "--help" ]; then
    usage
    exit 0
fi


while [[ $# > 2 ]]
do
    key="$2"

    case $key in
        -tb|--targetbranch)
        TARGET_BRANCH="$3"
        shift
        ;;

        *)
        echo "Unknown parameter: ${key}"
        echo
        usage
        ;;
    esac
    shift
done
#
# Enf reading of command line params.
#


# Script variables.
if [ "${APACHE_GIT}" = "" ]; then
    APACHE_GIT="https://gitbox.apache.org/repos/asf/ignite-extensions.git"
fi

if [ "${GITHUB_MIRROR}" = "" ]; then
    GITHUB_MIRROR="https://github.com/apache/ignite-extensions.git"
fi

if [ "${TARGET_BRANCH}" = "" ]; then
    TARGET_BRANCH="master"
fi

requireCleanWorkTree ${GIT_HOME}

CURRENT_BRANCH=`git rev-parse --abbrev-ref HEAD`

if [ "$CURRENT_BRANCH" != "${TARGET_BRANCH}" ]; then
    echo $0", ERROR:"
    echo "You have to be on ${TARGET_BRANCH} branch."

    exit 1
fi

# Check that target branch is up-to-date.
APACHE_GIT_TARGET_BRANCH="apache-git-target-br-tmp"

git fetch ${APACHE_GIT} ${TARGET_BRANCH}:${APACHE_GIT_TARGET_BRANCH} &> /dev/null
if test $? != 0; then
    echo $0", ERROR:"
    echo >&2 "Couldn't fetch '${TARGET_BRANCH}' branch from ${APACHE_GIT}."
    exit 1
fi

LOCAL_TARGET_BR_HASH=$(git rev-parse @)
REMOTE_TARGET_BR_HASH=$(git rev-parse ${APACHE_GIT_TARGET_BRANCH})
BASE_HASH=$(git merge-base @ ${APACHE_GIT_TARGET_BRANCH})

git branch -D ${APACHE_GIT_TARGET_BRANCH} &> /dev/null

if [ $LOCAL_TARGET_BR_HASH != $REMOTE_TARGET_BR_HASH ]; then
    echo $0", ERROR:"

    if [ $LOCAL_TARGET_BR_HASH = $BASE_HASH ]; then
        echo "Your local ${TARGET_BRANCH} branch is not up-to-date. You need to pull."
    elif [ $REMOTE_TARGET_BR_HASH = $BASE_HASH ]; then
        echo "Your local ${TARGET_BRANCH} branch is ahead of ${TARGET_BRANCH} branch at Apache git. You need to push."
    else
        echo "Your local ${TARGET_BRANCH} and Apache git ${TARGET_BRANCH} branches diverged. You need to pull, merge and pull."
    fi

    exit 1
fi

echo "Local ${TARGET_BRANCH} is Up-to-date."
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

# Update local target branch.
git checkout ${TARGET_BRANCH} &> /dev/null

# Take changes.
git merge --squash ${PR_BRANCH_NAME} &> /dev/null
if test $? != 0; then
    git reset --hard &> /dev/null

    echo $0", ERROR:"
    echo >&2 "Could not merge the pull-request to ${TARGET_BRANCH} without conflicts. All local changes have been discarded. You're on ${TARGET_BRANCH} branch."
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

COMMENT="${COMMENT} - Fixes #${PR_ID}."

if [ "${EXCLUDE_SPECIAL_FILE}" = "true" ]; then
    git checkout HEAD ignite-pull-request-id
fi

git commit --author "${AUTHOR}" -a -s -m "${COMMENT}" &> /dev/null

echo "Squash commit for pull request with id='${PR_ID}' has been added. The commit has been added with comment '${COMMENT}'."
echo "Now you can review changes of the last commit at ${TARGET_BRANCH} and push it into ${APACHE_GIT} git after."
echo "If you want to decline changes, you can remove the last commit from your repo by 'git reset --hard HEAD^'."
echo

# Clean-up.
git branch -D ${PR_BRANCH_NAME} &> /dev/null

echo 'Successfully completed.'
