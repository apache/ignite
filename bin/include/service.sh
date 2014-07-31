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

# Stop script on error.
set -e

#
# Resolve parameters.
#
MAIN_CLASS="org.gridgain.grid.startup.cmdline.GridCommandLineStartup"
GRIDGAIN_DEFAULTS="/etc/default/gridgain-hadoop"
DEFAULT_CONFIG="default-config.xml"

# Load GridGain path configuration
if [ -f "$GRIDGAIN_DEFAULTS" -a -r "$GRIDGAIN_DEFAULTS" ]; then
  source "$GRIDGAIN_DEFAULTS"
fi

# Load GridGain functions.
source "${GRIDGAIN_HOME}/bin/include/functions.sh"

# Configure GridGain environment.
source "${GRIDGAIN_HOME}/bin/include/setenv.sh"

# Set default JVM options if they was not passed.
if [ -z "$JVM_OPTS" ]; then
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts"
    [ "$HADOOP_EDITION" = "1" ] && JVM_OPTS="${JVM_OPTS} -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"
fi

# Resolve config directory.
GRIDGAIN_CONF_DIR=${GRIDGAIN_CONF_DIR-"${GRIDGAIN_HOME}/config"}

# Resolve full config path.
[[ "$DEFAULT_CONFIG" != /* ]] && DEFAULT_CONFIG="$GRIDGAIN_CONF_DIR/$DEFAULT_CONFIG"

# Discover path to Java executable and check it's version.
checkJava

# And run.
exec "$JAVA" $JVM_OPTS -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
    -DGRIDGAIN_PROG_NAME="$0" -cp "$GRIDGAIN_LIBS" "$MAIN_CLASS" "$DEFAULT_CONFIG"
