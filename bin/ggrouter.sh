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
# Import common functions.
#
if [ "${GRIDGAIN_HOME}" = "" ];
    then GRIDGAIN_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";GRIDGAIN_HOME_TMP="$(dirname "${GRIDGAIN_HOME_TMP}")"
    else GRIDGAIN_HOME_TMP=${GRIDGAIN_HOME};
fi

source "${GRIDGAIN_HOME_TMP}"/os/bin/include/functions.sh

#
# Discover GRIDGAIN_HOME environment variable.
#
setGridGainHome

#
# Get correct Java class path separator symbol for the given platform.
#
getClassPathSeparator

#
# Set router service environment.
#
export DEFAULT_CONFIG=config/router/default-router.xml
export MAIN_CLASS=org.gridgain.client.router.impl.GridRouterCommandLineStartup

#
# Start router service.
#
. "${GRIDGAIN_HOME}/os/bin/ggstart.sh" $@
