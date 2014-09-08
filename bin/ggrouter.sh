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
# Router command line loader.
#

#
# Set router service environment.
#
export DEFAULT_CONFIG=config/router/default-router.xml
export MAIN_CLASS=org.gridgain.client.router.impl.GridRouterCommandLineStartup

#
# Start router service.
#
. "$(dirname "$0")/ggstart.sh" $@
