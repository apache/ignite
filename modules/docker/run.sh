#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#
# Prepare execution
#
TMP=$(mktemp)
QUITE=""


#
# Add optional libs to classpath
#
if [ ! -z "${OPTION_LIBS}" ]
then
	IFS=, LIBS_LIST=("${OPTION_LIBS}")
	
	for lib in ${LIBS_LIST[@]}
	do
		cp -r "${IGNITE_HOME}/libs/optional/${lib}/"* \
        	      ${IGNITE_HOME}/libs/
	done
fi


#
# Add external libs to classpath
#
if [ ! -z "${EXTERNAL_LIBS}" ]
then
	IFS=, LIBS_LIST=("${EXTERNAL_LIBS}")

	for lib in ${LIBS_LIST[@]}
	do
		echo ${lib} >> ${TMP}
	done

	wget -i ${TMP} -P ${IGNITE_HOME}/libs
fi


#
# Set verbosity of Ignite execution
#
if ! ${IGNITE_QUIET}
then
	QUIET="-v"
fi


#
# Start Ignite
#
${IGNITE_HOME}/bin/ignite.sh ${QUIET} ${CONFIG_URI:-}


#
# Trap
#
trap "
    rm -rfv ${TMP}
" INT TERM ERR EXIT

