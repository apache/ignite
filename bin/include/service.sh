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

####################################################################
#            GridGain Hadoop service bootstrap script.
# Supposed to be called from unix `init.d` script. If environment
# is not passed to the script tries to read it using
# `/etc/default/gridgain-hadoop` file.
####################################################################

#
# Resolve parameters.
#
CONFIG_FILENAME="default-config.xml"
MAIN_CLASS="org.gridgain.grid.startup.cmdline.GridCommandLineStartup"
GRIDGAIN_DEFAULTS="/etc/default/gridgain-hadoop"
EXEC="java"

# Use JAVA_HOME if specified.
if [ -n "$JAVA_HOME" ]; then
  EXEC="$JAVA_HOME/bin/java"
fi

# Load GridGain path configuration if it wasn't passed through environment.
if [[ -z "$GRIDGAIN_HOME" && -f "$GRIDGAIN_DEFAULTS" ]]; then
  source "$GRIDGAIN_DEFAULTS"
fi

# Assume GridGain has simple deployment layout.
if [ -z "$GRIDGAIN_HOME" ]; then
  GRIDGAIN_HOME="$( cd "$( dirname "$0" )" && cd "../.." && pwd )"
fi

# Configure GridGain environment.
source "${GRIDGAIN_HOME}/bin/include/setenv.sh"

# Extend passed VM options.
GRIDGAIN_OPTS="$GRIDGAIN_OPTS -DGRIDGAIN_HOME=$GRIDGAIN_HOME -DGRIDGAIN_PROG_NAME=$0"

# And run.
exec "$EXEC" $GRIDGAIN_OPTS -cp "$GRIDGAIN_LIBS" "$MAIN_CLASS" "$GRIDGAIN_CONF_DIR/$CONFIG_FILENAME"
