#!/bin/bash
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
# Router command line loader.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ]; then 
    IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")"
else 
    IGNITE_HOME_TMP=${IGNITE_HOME}
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set router service environment.
#
export DEFAULT_CONFIG=config/router/default-router.xml
export MAIN_CLASS=org.apache.ignite.internal.client.router.impl.GridRouterCommandLineStartup

#
# Start router service.
#
. "${SCRIPTS_HOME}"/ignite.sh $@
