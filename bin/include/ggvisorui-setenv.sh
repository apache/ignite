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
# Check for "gridgain-ggfs" module and try to add HADOOP libs in classpath if needed.
#

if [ "${HADOOP_EDITION}" != "1" ]; then
    for file in ${GRIDGAIN_HOME}/libs/*
    do
        if [ -d ${file} ] && [ "${file}" == "${GRIDGAIN_HOME}"/libs/gridgain-hadoop ]; then
            HAS_GGFS=1
        fi
    done

    if [ "${HAS_GGFS}" == "1" ]; then
        . "${SCRIPTS_HOME}"/include/hadoop-classpath.sh

        if [ "${GRIDGAIN_HADOOP_CLASSPATH}" != "" ]; then
            GRIDGAIN_LIBS=${GRIDGAIN_LIBS}${SEP}$GRIDGAIN_HADOOP_CLASSPATH
        fi
    fi
fi
