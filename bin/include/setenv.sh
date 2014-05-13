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
# Exports GRIDGAIN_LIBS variable containing classpath for GridGain.
# Expects GRIDGAIN_HOME to be set.
# Can be used like:
#       . "${GRIDGAIN_HOME}"/bin/include/setenv.sh
# in other scripts to set classpath using exported GRIDGAIN_LIBS variable.
#

#
# Check GRIDGAIN_HOME.
#
if [ "${GRIDGAIN_HOME}" = "" ]; then
    echo $0", ERROR: GridGain installation folder is not found."
    echo "Please create GRIDGAIN_HOME variable pointing to location of"
    echo "GridGain installation folder."

    exit 1
fi

# USER_LIBS variable can optionally contain user's JARs/libs.
USER_LIBS=.

#
# OS specific support.
#
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

# The following libraries are required for GridGain.
GRIDGAIN_LIBS=${GRIDGAIN_HOME}/config/userversion${SEP}${GRIDGAIN_HOME}/libs/*${SEP}${GRIDGAIN_HOME}/libs/${HADOOP_LIB_DIR}/*

if [ "${USER_LIBS}" != "" ]; then
    GRIDGAIN_LIBS=${USER_LIBS}${SEP}${GRIDGAIN_LIBS}
fi

#
# Set property JAR name during the Ant build.
#
ANT_AUGMENTED_GGJAR=gridgain.jar

GRIDGAIN_LIBS=${GRIDGAIN_LIBS}${SEP}${GRIDGAIN_HOME}/${ANT_AUGMENTED_GGJAR}

# Uncomment if using JBoss.
# JBOSS_HOME must point to JBoss installation folder.
# JBOSS_HOME=

# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/lib/jboss-common.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/lib/jboss-jmx.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/lib/jboss-system.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jbossha.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jboss-j2ee.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jboss.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jboss-transaction.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jmx-adaptor-plugin.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/server/all/lib/jnpserver.jar

# If using JBoss AOP following libraries need to be downloaded separately
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/lib/jboss-aop-jdk50.jar
# GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${JBOSS_HOME}"/lib/jboss-aspect-library-jdk50.jar

# Set user external libraries
GRIDGAIN_LIBS="${GRIDGAIN_LIBS}${SEP}${GRIDGAIN_HOME}/libs/ext/*"
