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
# This script starts nodes in SSH session.
#
# It resolves MacOS X problem on TeamCity to start nodes with direct script call:
# nohup: can't detach from console: Inappropriate ioctl for device
#

# Define environment paths.
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)
IGNITE_HOME=$(cd $SCRIPT_DIR/../../..; pwd)

SSH_COMMAND="ssh localhost /bin/bash $SCRIPT_DIR/start-nodes.sh"

echo Execute command in ssh session:
echo $SSH_COMMAND
echo

# Require ~/.ssh/authorized_keys to be defined for current user.
$SSH_COMMAND

echo
echo SSH session closed.
