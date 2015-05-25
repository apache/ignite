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
# Copy this script file at root of ignite repo.
# Fill all variables.
#

#
# Default branch name. Need to use last version of "jiraslurp" scripts.
#
DEFAULT_BRANCH='ignite-sprint-5'

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

#
# Space separated logins of jira useres which were approved to auto triggering patches. Like 'user1 user2 user3'.
#
JIRA_CONTRIBUTORS=''

git fetch

git checkout ${DEFAULT_BRANCH}

git pull

export TC_URL=${TC_URL}
export GIT_USER_NAME=${GIT_USER_NAME}
export GIT_USER_EMAIL=${GIT_USER_EMAIL}
export JIRA_USER=${JIRA_USER}
export JIRA_PWD=${JIRA_PWD}
export TASK_RUNNER_PWD=${TASK_RUNNER_PWD}
export TASK_RUNNER_USER=${TASK_RUNNER_USER}
export JIRA_CONTRIBUTORS=${JIRA_CONTRIBUTORS}

gradle slurp -b dev-tools/build.gradle
