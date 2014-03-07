#!/bin/sh
#
# @sh.file.header
#
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#

osname=`uname`

case $osname in
    Darwin*)
        export MODULE_DIR=$(dirname $(dirname $(cd ${0%/*} && echo $PWD/${0##*/})))
        ;;
    *)
        export MODULE_DIR="$(dirname $(readlink -f $0))"/..
        ;;
esac

export GRIDGAIN_HOME="${MODULE_DIR}"/../..

"${GRIDGAIN_HOME}"/bin/ggstart.sh -v modules/tests/config/spring-start-nodes-attr.xml
