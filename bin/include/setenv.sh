#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace

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

#
# Exports IGNITE_LIBS variable containing classpath for Ignite.
# Expects IGNITE_HOME to be set.
# Can be used like:
#       . "${IGNITE_HOME}"/bin/include/setenv.sh
# in other scripts to set classpath using exported IGNITE_LIBS variable.
#

#
# Check IGNITE_HOME.
#
if [ "${IGNITE_HOME:-}" = "" ]; then
    echo $0", ERROR: Ignite installation folder is not found."
    echo "Please create IGNITE_HOME variable pointing to location of"
    echo "Ignite installation folder."

    exit 1
fi

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

#
# Libraries included in classpath.
#
IGNITE_LIBS="${IGNITE_HOME}/libs/*"

SAVEIFS=$IFS
IFS=$(echo -en "\n\b")

for file in ${IGNITE_HOME}/libs/*
do
    if [ -d ${file} ] && [ "${file}" != "${IGNITE_HOME}"/libs/optional ]; then
        IGNITE_LIBS=${IGNITE_LIBS:-}${SEP}${file}/*
    fi
done

IFS=$SAVEIFS

if [ "${USER_LIBS:-}" != "" ]; then
    IGNITE_LIBS=${USER_LIBS:-}${SEP}${IGNITE_LIBS}
fi
