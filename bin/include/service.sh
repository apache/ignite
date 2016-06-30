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

####################################################################
#            Ignite Hadoop service start/stop script.
# Supposed to be called from unix `init.d` script. Environment must
# be set via the call of /etc/default/{hadoop,ignite-hadoop}
####################################################################

# Stop script on error.
set -e

# Include LSB init functions.
. /lib/lsb/init-functions

# Service name.
SERVICE=$2

# Name of PID file.
PIDFILE=${IGNITE_PID_DIR}/${SERVICE}.pid

case "$1" in
    start)
        #
        # Resolve parameters.
        #
        MAIN_CLASS="org.apache.ignite.startup.cmdline.CommandLineStartup"
        DEFAULT_CONFIG="default-config.xml"

        # Is needed for setenv
        SCRIPTS_HOME=${IGNITE_HOME}/bin

        # Load Ignite functions.
        source "${SCRIPTS_HOME}/include/functions.sh"

        # Configure Ignite environment.
        source "${SCRIPTS_HOME}/include/setenv.sh"

        # Set default JVM options if they was not passed.
        if [ -z "$JVM_OPTS" ]; then
            JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts -XX:MaxPermSize=512m"
        fi

        # Resolve config directory.
        IGNITE_CONF_DIR=${IGNITE_CONF_DIR-"${IGNITE_HOME}/config"}

        # Resolve full config path.
        [[ "$DEFAULT_CONFIG" != /* ]] && DEFAULT_CONFIG="$IGNITE_CONF_DIR/$DEFAULT_CONFIG"

        # Discover path to Java executable and check it's version.
        checkJava

        # Clear output log.
        echo >"${IGNITE_LOG_DIR}"/ignite.out

        # And run.
        $JAVA $JVM_OPTS -DIGNITE_UPDATE_NOTIFIER=false -DIGNITE_HOME="${IGNITE_HOME}" \
          -DIGNITE_PROG_NAME="$0" -cp "$IGNITE_LIBS" "$MAIN_CLASS" "$DEFAULT_CONFIG" >& "${IGNITE_LOG_DIR}"/ignite.out &

        # Write process id.
        echo $! >$PIDFILE
    ;;
    stop)
        killproc -p $PIDFILE java
    ;;
    *)
        echo $"Usage: $0 {start|stop} SERVICE_NAME"
esac
