#!/usr/bin/env bash
set -o nounset
set -o errexit
set -o pipefail
set -o errtrace
set -o functrace
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

CONFIG=${DEFAULT_CONFIG:-}
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

#
# Set 'file.encoding' to UTF-8 default if not specified otherwise
#
case "${JVM_OPTS:-}" in
    *-Dfile.encoding=*)
        ;;
    *)
        JVM_OPTS="${JVM_OPTS:-} -Dfile.encoding=UTF-8";;
esac
