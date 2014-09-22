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
# Grid command line loader.
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
# Discover path to Java executable and check it's version.
#
checkJava

#
# Discover GRIDGAIN_HOME environment variable.
#
setGridGainHome

if [ "${DEFAULT_CONFIG}" == "" ]; then
    DEFAULT_CONFIG="${GRIDGAIN_HOME}/os/config/default-config.xml"
fi

#
# Parse command line parameters.
#
. "${GRIDGAIN_HOME}"/os/bin/include/parseargs.sh

#
# Set GRIDGAIN_LIBS.
#
. "${GRIDGAIN_HOME}"/os/bin/include/setenv.sh
. "${GRIDGAIN_HOME}/os/bin/include/target-classpath.sh"
CP="${GRIDGAIN_LIBS}"

RANDOM_NUMBER=$("$JAVA" -cp "${CP}" org.gridgain.grid.startup.cmdline.GridCommandLineRandomNumberGenerator)

RESTART_SUCCESS_FILE="${GRIDGAIN_HOME}/work/gridgain_success_${RANDOM_NUMBER}"
RESTART_SUCCESS_OPT="-DGRIDGAIN_SUCCESS_FILE=${RESTART_SUCCESS_FILE}"

#
# Find available port for JMX
#
# You can specify GRIDGAIN_JMX_PORT environment variable for overriding automatically found JMX port
#
findAvailableJmxPort

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS}" == "" ]; then
    DOCK_OPTS="-Xdock:name=GridGain Node"
fi

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
if [ -z "$JVM_OPTS" ] ; then
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:+AggressiveOpts"

    # Hadoop needs class unloading enabled and bigger PermGen size.
    if [ "GRIDGAIN_HADOOP_CLASSPATH" != "" ]; then
        JVM_OPTS="$JVM_OPTS -XX:MaxPermSize=350m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled"
        JVM_OPTS="$JVM_OPTS -XX:+ParallelRefProcEnabled -XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses"
    fi
fi

#
# Uncomment the following GC settings if you see spikes in your throughput due to Garbage Collection.
#
# JVM_OPTS="$JVM_OPTS -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseTLAB -XX:NewSize=128m -XX:MaxNewSize=128m"
# JVM_OPTS="$JVM_OPTS -XX:MaxTenuringThreshold=0 -XX:SurvivorRatio=1024 -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=60"

#
# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
#
# JVM_OPTS="${JVM_OPTS} -Xss4m"

#
# Uncomment to set preference for IPv4 stack.
#
# JVM_OPTS="${JVM_OPTS} -Djava.net.preferIPv4Stack=true"

#
# Assertions are disabled by default since version 3.5.
# If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
#
ENABLE_ASSERTIONS="1"

#
# Set '-ea' options if assertions are enabled.
#
if [ "${ENABLE_ASSERTIONS}" = "1" ]; then
    JVM_OPTS="${JVM_OPTS} -ea"
fi

#
# Set main class to start service (grid node by default).
#
if [ "${MAIN_CLASS}" = "" ]; then
    MAIN_CLASS=org.gridgain.grid.startup.cmdline.GridCommandLineStartup
fi

#
# Remote debugging (JPDA).
# Uncomment and change if remote debugging is required.
#
# JVM_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n ${JVM_OPTS}"

ERRORCODE="-1"

while [ "${ERRORCODE}" -ne "130" ]
do
    if [ "${INTERACTIVE}" == "1" ] ; then
        case $osname in
            Darwin*)
                "$JAVA" ${JVM_OPTS} ${QUIET} "${DOCK_OPTS}" "${RESTART_SUCCESS_OPT}" ${JMX_MON} \
                -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
                -DGRIDGAIN_PROG_NAME="$0" ${JVM_XOPTS} -cp "${CP}" ${MAIN_CLASS}
            ;;
            *)
                "$JAVA" ${JVM_OPTS} ${QUIET} "${RESTART_SUCCESS_OPT}" ${JMX_MON} \
                -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
                -DGRIDGAIN_PROG_NAME="$0" ${JVM_XOPTS} -cp "${CP}" ${MAIN_CLASS}
            ;;
        esac
    else
        case $osname in
            Darwin*)
                "$JAVA" ${JVM_OPTS} ${QUIET} "${DOCK_OPTS}" "${RESTART_SUCCESS_OPT}" ${JMX_MON} \
                 -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
                 -DGRIDGAIN_PROG_NAME="$0" ${JVM_XOPTS} -cp "${CP}" ${MAIN_CLASS} "${CONFIG}"
            ;;
            *)
                "$JAVA" ${JVM_OPTS} ${QUIET} "${RESTART_SUCCESS_OPT}" ${JMX_MON} \
                 -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
                 -DGRIDGAIN_PROG_NAME="$0" ${JVM_XOPTS} -cp "${CP}" ${MAIN_CLASS} "${CONFIG}"
            ;;
        esac
    fi

    ERRORCODE="$?"

    if [ ! -f "${RESTART_SUCCESS_FILE}" ] ; then
        break
    else
        rm -f "${RESTART_SUCCESS_FILE}"
    fi
done

if [ -f "${RESTART_SUCCESS_FILE}" ] ; then
    rm -f "${RESTART_SUCCESS_FILE}"
fi
