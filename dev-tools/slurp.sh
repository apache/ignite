#!/bin/bash
#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Copy this script file at root of ignite repo.
# Fill all variables.
#

#
# Default branch name. Need to use last version of "jiraslurp" scripts.
#
DEFAULT_BRANCH='master'

#
# TC URL. It is 10.30.0.229 for public TC from agents.
#
TC_URL='10.30.0.229'

#
# Jira user name to add comments aboyt triggered builds.
#
JIRA_USER='tc_commenter'

#
# Jira password.
#
JIRA_PWD=''

#
# TC user which have permissions to trigger new builds.
#
TASK_RUNNER_USER='task_runner'

#
# TC user password.
#
TASK_RUNNER_PWD=''

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "<"$(date + "%D - %H:%M:%S")"> Starting task triggering"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"

# Useful settings
#cd /home/teamcity/jobs/ignite/
#
#export JAVA_HOME=<java_home>
#export PATH=$PATH:<gradle_path>

git fetch

git checkout ${DEFAULT_BRANCH}

git pull

export TC_URL=${TC_URL}
export JIRA_USER=${JIRA_USER}
export JIRA_PWD=${JIRA_PWD}
export TASK_RUNNER_PWD=${TASK_RUNNER_PWD}
export TASK_RUNNER_USER=${TASK_RUNNER_USER}

gradle slurp -b dev-tools/build.gradle
