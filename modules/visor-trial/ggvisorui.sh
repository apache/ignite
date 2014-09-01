#!/bin/bash
#
# Copyright (C) GridGain Systems. All Rights Reserved.
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: 6.2.0
#

#
# Starts Visor Dashboard with GridGain on the classpath.
#

#
# Import common functions.
#
if [ "${GRIDGAIN_HOME}" = "" ];
    then GRIDGAIN_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else GRIDGAIN_HOME_TMP=${GRIDGAIN_HOME};
fi

source "${GRIDGAIN_HOME_TMP}"/bin/include/functions.sh

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
. "${GRIDGAIN_HOME}"/bin/include/parseargs.sh

#
# Set GRIDGAIN_LIBS.
#
. "${GRIDGAIN_HOME}"/bin/include/setenv.sh

CP="${GRIDGAIN_LIBS}${SEP}${GRIDGAIN_HOME}/bin/include/visorui/*"

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp
# for more details. Note that default settings use ** PARALLEL GC**.
#
# NOTE
# ====
# ASSERTIONS ARE DISABLED BY DEFAULT SINCE VERSION 3.5.
# IF YOU WANT TO ENABLE THEM - ADD '-ea' TO JVM_OPTS VARIABLE
#
# ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
#
JVM_OPTS="-Xms1g -Xmx1g -Xss1m -XX:NewSize=64m -XX:MaxNewSize=64m -XX:PermSize=128m \
-XX:MaxPermSize=128m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0 -XX:+UseTLAB \
-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled ${JVM_OPTS}"

# Mac OS specific support to display correct name in the dock.
osname=`uname`

if [ "${DOCK_OPTS}" == "" ]; then
    DOCK_OPTS="-Xdock:name=Visor - GridGain Admin Console"
fi

if [ -z "$MAC_OS_OPTS" ] ; then
    MAC_OS_OPTS=-Dsun.java2d.opengl=false
fi

# Uncomment to set preference for IPv4 stack.
# JVM_OPTS="${JVM_OPTS} -Djava.net.preferIPv4Stack=true"

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
# Set Visor plugins directory.
#
VISOR_PLUGINS_DIR="${GRIDGAIN_HOME}/bin/include/visorui/plugins"

#
# Starts Visor Dashboard.
#
case $osname in
    Darwin*)
        "$JAVA" ${JVM_OPTS} ${QUIET} ${MAC_OS_OPTS} "${DOCK_OPTS}" -DGRIDGAIN_PERFORMANCE_SUGGESTIONS_DISABLED=true \
        -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
        -DGRIDGAIN_PROG_NAME="$0" ${JVM_XOPTS} -cp "${CP}" \
        -Dpf4j.pluginsDir="${VISOR_PLUGINS_DIR}" \
        org.gridgain.visor.gui.VisorGuiLauncher
    ;;
    *)
        "$JAVA" ${JVM_OPTS} ${QUIET} -DGRIDGAIN_PERFORMANCE_SUGGESTIONS_DISABLED=true \
        -DGRIDGAIN_UPDATE_NOTIFIER=false -DGRIDGAIN_HOME="${GRIDGAIN_HOME}" \
        -DGRIDGAIN_PROG_NAME="$0" -DGRIDGAIN_DEPLOYMENT_MODE_OVERRIDE=ISOLATED ${JVM_XOPTS} -cp "${CP}" \
        -Dpf4j.pluginsDir="${VISOR_PLUGINS_DIR}" \
        org.gridgain.visor.gui.VisorGuiLauncher
    ;;
esac

#
# Restore terminal.
#
restoreSttySettings
