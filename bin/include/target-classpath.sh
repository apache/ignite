#!/bin/sh
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

# Target class path resolver.
#
# Can be used like:
#       . "${GRIDGAIN_HOME}"/os/bin/include/target-classpath.sh
# in other scripts to set classpath using libs from target folder.
#
# Will be excluded in release.


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

includeToClassPath() {
    for file in $1/*
    do
        if [ -d ${file} ] && [ -d "${file}/target" ]; then
            if [ -d "${file}/target/classes" ]; then
                GRIDGAIN_LIBS=${GRIDGAIN_LIBS}${SEP}${file}/target/classes
            fi

            if [ -d "${file}/target/libs" ]; then
                GRIDGAIN_LIBS=${GRIDGAIN_LIBS}${SEP}${file}/target/libs/*
            fi
        fi
    done
}

#
# Include target libraries for opensourse modules to classpath.
#
includeToClassPath ${GRIDGAIN_HOME}/os/modules

#
# Include target libraries for enterprise modules to classpath.
#
includeToClassPath ${GRIDGAIN_HOME}/modules
