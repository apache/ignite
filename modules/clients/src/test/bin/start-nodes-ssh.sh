#!/bin/bash
#
# @sh.file.header
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: @sh.file.version
#

#
# This script starts nodes in SSH session.
#
# It resolves MacOS X problem on TeamCity to start nodes with direct script call:
# nohup: can't detach from console: Inappropriate ioctl for device
#

# Define environment paths.
SCRIPT_DIR=$(cd $(dirname "$0"); pwd)
GG_HOME=$(cd $SCRIPT_DIR/../../..; pwd)

SSH_COMMAND="ssh localhost /bin/bash $SCRIPT_DIR/start-nodes.sh"

echo Execute command in ssh session:
echo $SSH_COMMAND
echo

# Require ~/.ssh/authorized_keys to be defined for current user.
$SSH_COMMAND

echo
echo SSH session closed.
