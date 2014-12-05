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
#            GridGain Hadoop service start/stop script.
# Supposed to be called from unix `init.d` script. Environment must
# be set via the call of /etc/default/{hadoop,gridgain-hadoop}
####################################################################

# Stop script on error.
set -e

# Include LSB init functions.
. /lib/lsb/init-functions

# Service name.
SERVICE=$2

# Name of PID file.
PIDFILE=${GRIDGAIN_PID_DIR}/${SERVICE}.pid

case "$1" in
    start)
        #
        # Resolve parameters.
        #
        MAIN_CLASS="org.apache.ignite.startup.cmdline.CommandLineStartup"
        DEFAULT_CONFIG="default-config.xml"

        # Is needed for setenv
        SCRIPTS_HOME=${GRIDGAIN_HOME}/bin

        # Load GridGain functions.
        source "${SCRIPTS_HOME}/include/functions.sh"

        # Configure GridGain environment.
        source "${SCRIPTS_HOME}/include/setenv.sh"

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
        $JAVA $JVM_OPTS -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
        -DGRIDGAIN_PROG_NAME="$0" -cp "$GRIDGAIN_LIBS" "$MAIN_CLASS" "$DEFAULT_CONFIG" &>/dev/null &

        # Write process id.
        echo $! >$PIDFILE
    ;;
    stop)
        killproc -p $PIDFILE java
    ;;
    *)
        echo $"Usage: $0 {start|stop} SERVICE_NAME"
esac
