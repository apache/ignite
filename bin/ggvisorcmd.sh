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
# Starts GridGain Visor Console.
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

#
# Parse command line parameters.
#
. "${GRIDGAIN_HOME}"/os/bin/include/parseargs.sh

#
# Set GRIDGAIN_LIBS.
#
. "${GRIDGAIN_HOME}"/os/bin/include/setenv.sh
. "${GRIDGAIN_HOME}/os/bin/include/target-classpath.sh"
CP="${GRIDGAIN_HOME}/bin/include/visorcmd/*${SEP}${GRIDGAIN_LIBS}"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp for more details.
#
# ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
#
JVM_OPTS="-Xms1g -Xmx1g -XX:MaxPermSize=128M -server ${JVM_OPTS}"

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Visor - GridGain Shell Console"
fi

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
# Save terminal setting. Used to restore terminal on finish.
#
SAVED_STTY=`stty -g 2>/dev/null`

#
# Restores terminal.
#
function restoreSttySettings() {
    stty ${SAVED_STTY}
}

#
# Trap that restores terminal in case script execution is interrupted.
#
trap restoreSttySettings INT

#
# Start Visor console.
#
case $osname in
    Darwin*)
        "$JAVA" ${JVM_OPTS} ${QUIET} "${DOCK_OPTS}" \
        -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" -DGRIDGAIN_PROG_NAME="$0" \
        -DGRIDGAIN_DEPLOYMENT_MODE_OVERRIDE=ISOLATED ${JVM_XOPTS} -cp "${CP}" \
        org.gridgain.visor.commands.VisorConsole
    ;;
    *)
        "$JAVA" ${JVM_OPTS} ${QUIET} -DGRIDGAIN_UPDATE_NOTIFIER=false \
        -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" -DGRIDGAIN_PROG_NAME="$0" -DGRIDGAIN_DEPLOYMENT_MODE_OVERRIDE=ISOLATED \
        ${JVM_XOPTS} -cp "${CP}" \
        org.gridgain.visor.commands.VisorConsole
    ;;
esac

#
# Restore terminal.
#
restoreSttySettings
