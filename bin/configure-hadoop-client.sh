#!/bin/bash
#
# Copyright (C) GridGain Systems. All Rights Reserved.
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: 6.1.8
#

#
# Add simbolyc links to gridgain libs into hadoop libs.
# Run this script to confure hadoop client to work with gridgain.
#

#
# Check HADOOP_HOME
#

HADOOP_COMMON_HOME=

if [ "$HADOOP_HOME" == "" ]; then
    #Try get all variables from /etc/default
    HADOOP_DEFAULTS=/etc/default/hadoop

    if [ -f $HADOOP_DEFAULTS ]; then
        . $HADOOP_DEFAULTS
    fi
fi

if [ "$HADOOP_HOME" == "" ]; then
    echo ERROR: HADOOP_HOME variable is not set.
    exit 1
fi

echo
echo "Found Hadoop in $HADOOP_HOME"
echo

#
# Setting all hadoop modules if it's not set by /etc/default/hadoop
#
if [ "$HADOOP_COMMON_HOME" == "" ]; then
    export HADOOP_COMMON_HOME=$HADOOP_HOME/share/hadoop/common
fi

#
# Import common functions.
#
if [ "${GRIDGAIN_HOME}" = "" ];
    then GRIDGAIN_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")";
    else GRIDGAIN_HOME_TMP=${GRIDGAIN_HOME};
fi

source "${GRIDGAIN_HOME_TMP}"/bin/include/functions.sh

#
# Discover GRIDGAIN_HOME environment variable.
#
setGridGainHome

GG_JAR=$(ls ${GRIDGAIN_HOME}/libs/gridgain-*.jar)
GG_JAR=$(readlink -m $GG_JAR)

GGH_JAR=$(ls ${GRIDGAIN_HOME}/libs/gridgain-hadoop/gridgain-hadoop-*.jar)
GGH_JAR=$(readlink -m $GGH_JAR)

COMMON_HOME_LIB=$HADOOP_COMMON_HOME/lib

rm  ${COMMON_HOME_LIB}/gridgain* 2>/dev/null

ln -s $GG_JAR ${COMMON_HOME_LIB}/gridgain.jar
ln -s ${GGH_JAR} ${COMMON_HOME_LIB}/gridgain-hadoop.jar
