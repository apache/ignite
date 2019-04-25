#!/bin/sh
#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Target class path resolver.
#
# Can be used like:
#       . "${IGNITE_HOME}"/bin/include/build-classpath.sh
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
        export IGNITE_HOME=`echo $IGNITE_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
        ;;
    CYGWIN*)
        SEP=";";
        export IGNITE_HOME=`echo $IGNITE_HOME | sed -e 's/^\/\([a-zA-Z]\)/\1:/'`
        ;;
esac

includeToClassPath() {
    SAVEIFS=$IFS
    IFS=$(echo -en "\n\b")

    for file in $1/*
    do
        if [ -d ${file} ] && [ -d "${file}/target" ]; then
            if [ -d "${file}/target/classes" ]; then
                IGNITE_LIBS=${IGNITE_LIBS}${SEP}${file}/target/classes
            fi

            if [ -d "${file}/target/test-classes" ]; then
                IGNITE_LIBS=${IGNITE_LIBS}${SEP}${file}/target/test-classes
            fi

            if [ -d "${file}/target/libs" ]; then
                IGNITE_LIBS=${IGNITE_LIBS}${SEP}${file}/target/libs/*
            fi
        fi
    done
    
    IFS=$SAVEIFS
}

#
# Include target libraries for enterprise modules to classpath.
#
includeToClassPath modules

#
# Include target libraries for opensourse modules to classpath.
#
includeToClassPath "${IGNITE_HOME}"/modules
