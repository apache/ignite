#!/bin/bash
#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
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
