#!/bin/bash
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
# Parses command line parameters into Ignite variables that are common for the launcher scripts:
# CONFIG
# INTERACTIVE
# QUIET
# JVM_XOPTS
# NOJMX
#
# Script setups reasonable defaults (see below) for omitted arguments.
#
# Scripts accepts following incoming variables:
# DEFAULT_CONFIG
#
# Can be used like:
#       . "${IGNITE_HOME}"/bin/include/parseargs.sh
# in other scripts to parse common command lines parameters.
#

CONFIG=${DEFAULT_CONFIG}
INTERACTIVE="0"
NOJMX="0"
QUIET="-DIGNITE_QUIET=true"
JVM_XOPTS=""

while [ $# -gt 0 ]
do
    case "$1" in
        -i) INTERACTIVE="1";;
        -nojmx) NOJMX="1";;
        -v) QUIET="-DIGNITE_QUIET=false";;
        -J*) JVM_XOPTS="$JVM_XOPTS ${1:2}";;
        *) CONFIG="$1";;
    esac
    shift
done
