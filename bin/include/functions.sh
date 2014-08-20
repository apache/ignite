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
# This is a collection of utility functions to be used in other GridGain scripts.
# Before calling any function from this file you have to import it:
#   if [ "${GRIDGAIN_HOME}" = "" ];
#       then GRIDGAIN_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
#       else GRIDGAIN_HOME_TMP=${GRIDGAIN_HOME};
#   fi
#
#   source "${GRIDGAIN_HOME_TMP}"/bin/include/functions.sh
#

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    # Check JAVA_HOME.
    if [ "$JAVA_HOME" = "" ]; then
        JAVA=`which java`
        RETCODE=$?

        if [ $RETCODE -ne 0 ]; then
            echo $0", ERROR:"
            echo "JAVA_HOME environment variable is not found."
            echo "Please point JAVA_HOME variable to location of JDK 1.7 or JDK 1.8."
            echo "You can also download latest JDK at http://java.com/download"

            exit 1
        fi

        JAVA_HOME=
    else
        JAVA=${JAVA_HOME}/bin/java
    fi

    #
    # Check JDK.
    #
    if [ ! -e "$JAVA" ]; then
        echo $0", ERROR:"
        echo "JAVA is not found in JAVA_HOME=$JAVA_HOME."
        echo "Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8."
        echo "You can also download latest JDK at http://java.com/download"

        exit 1
    fi

    JAVA_VER=`"$JAVA" -version 2>&1 | egrep "1\.[78]\."`

    if [ "$JAVA_VER" == "" ]; then
        echo $0", ERROR:"
        echo "The version of JAVA installed in JAVA_HOME=$JAVA_HOME is incorrect."
        echo "Please point JAVA_HOME variable to installation of JDK 1.7 or JDK 1.8."
        echo "You can also download latest JDK at http://java.com/download"

        exit 1
    fi
}

#
# Discovers GRIDGAIN_HOME environment variable.
# The function expects GRIDGAIN_HOME_TMP variable is set and points to the directory where the callee script resides.
# The function exports GRIDGAIN_HOME variable with path to GridGain home directory.
#
setGridGainHome() {
    #
    # Set GRIDGAIN_HOME, if needed.
    #
    if [ "${GRIDGAIN_HOME}" = "" ]; then
        export GRIDGAIN_HOME=${GRIDGAIN_HOME_TMP}
    fi

    #
    # Check GRIDGAIN_HOME is valid.
    #
    if [ ! -d "${GRIDGAIN_HOME}/config" ]; then
        echo $0", ERROR:"
        echo "GridGain installation folder is not found or GRIDGAIN_HOME environment variable is not valid."
        echo "Please create GRIDGAIN_HOME environment variable pointing to location of GridGain installation folder."

        exit 1
    fi

    #
    # Check GRIDGAIN_HOME points to current installation.
    #
    if [ "${GRIDGAIN_HOME}" != "${GRIDGAIN_HOME_TMP}" ] &&
       [ "${GRIDGAIN_HOME}" != "${GRIDGAIN_HOME_TMP}/" ]; then
        echo $0", WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: $GRIDGAIN_HOME"
    fi
}

#
# Finds available port for JMX.
# The function exports JMX_MON variable with Java JMX options.
#
findAvailableJmxPort() {
    JMX_PORT=`"$JAVA" -cp "${GRIDGAIN_LIBS}" org.gridgain.grid.util.portscanner.GridJmxPortFinder`

    #
    # This variable defines necessary parameters for JMX
    # monitoring and management.
    #
    # This enables remote unsecure access to JConsole or VisualVM.
    #
    # ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
    #
    if [ -n "$JMX_PORT" ]; then
        JMX_MON="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=${JMX_PORT} \
            -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
    else
        # If JMX port wasn't found do not initialize JMX.
        echo "$0, WARN: Failed to resolve JMX host (JMX will be disabled): $HOSTNAME"
        JMX_MON=""
    fi
}

#
# Gets correct Java class path separator symbol for the given platform.
# The function exports SEP variable with class path separator symbol.
#
getClassPathSeparator() {
    SEP=":";

    case "`uname`" in
        MINGW*)
            SEP=";";
            export GRIDGAIN_HOME=`echo $GRIDGAIN_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
            ;;
        CYGWIN*)
            SEP=";";
            export GRIDGAIN_HOME=`echo $GRIDGAIN_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
            ;;
    esac
}
