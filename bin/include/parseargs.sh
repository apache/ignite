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
# Parses command line parameters into GridGain variables that are common for the launcher scripts:
# CONFIG
# INTERACTIVE
# QUIET
# JVM_XOPTS
#
# Script setups reasonable defaults (see below) for omitted arguments.
#
# Scripts accepts following incoming variables:
# DEFAULT_CONFIG
#
# Can be used like:
#       . "${GRIDGAIN_HOME}"/bin/include/parseargs.sh
# in other scripts to parse common command lines parameters.
#

CONFIG=${DEFAULT_CONFIG}
INTERACTIVE="0"
QUIET="-DGRIDGAIN_QUIET=true"
JVM_XOPTS=""

while [ $# -gt 0 ]
do
    case "$1" in
        -i) INTERACTIVE="1";;
        -v) QUIET="-DGRIDGAIN_QUIET=false";;
        -J*) JVM_XOPTS="$JVM_XOPTS ${1:2}";;
        *) CONFIG="$1";;
    esac
    shift
done
