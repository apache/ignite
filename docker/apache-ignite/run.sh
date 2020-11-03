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
source "${IGNITE_HOME}"/bin/include/functions.sh

#
# Discover path to Java executable and check it's version.
#
checkJava

#
# Set IGNITE_LIBS.
#
source "${IGNITE_HOME}"/bin/include/setenv.sh
CP="${IGNITE_LIBS}"
DEFAULT_CONFIG=config/default-config.xml

#
# Add optional libs to classpath
#
if [ -n "${OPTION_LIBS}" ]; then
    IFS=, LIBS_LIST=("$(tr -d '[:space:]' <<< ${OPTION_LIBS})")
  for lib in ${LIBS_LIST[@]}; do
    LIBS=$(JARS=("${IGNITE_HOME}/libs/optional/${lib}"/*); IFS=:; echo "${JARS[*]}")
    if [ -z "${USER_LIBS}" ]; then
      export USER_LIBS="${LIBS}"
    else
      export USER_LIBS="${USER_LIBS}:${LIBS}"
    fi
  done
fi

#
# Add external libs to classpath
#
if [ -n "${EXTERNAL_LIBS}" ]; then
  IFS=, LIBS_LIST=("${EXTERNAL_LIBS}")
  for lib in "${LIBS_LIST[@]}"; do
    echo "${lib}" >> "${IGNITE_HOME}"/work/external_libs
  done
  wget --content-disposition -i "${IGNITE_HOME}"/work/external_libs -P "${IGNITE_HOME}"/libs/external
  rm "${IGNITE_HOME}"/work/external_libs
fi

#
# Define classpath
#
if [ "${USER_LIBS:-}" != "" ]; then
    IGNITE_LIBS=${USER_LIBS:-}:${IGNITE_LIBS}
fi
CP="${IGNITE_LIBS}"
unset IFS

#
# Define default Java options
#
if [ -z "${JVM_OPTS}" ] ; then
    JVM_OPTS="-Xms1g -Xmx1g -server -XX:MaxMetaspaceSize=256m"
fi

#
# Add Java extra option 
#
if [ "${version}" -eq 8 ] ; then
    JVM_OPTS="\
        -XX:+AggressiveOpts \
         ${JVM_OPTS}"
elif [ "${version}" -gt 8 ] && [ "${version}" -lt 11 ]; then
    JVM_OPTS="\
        -XX:+AggressiveOpts \
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --illegal-access=permit \
        --add-modules=java.xml.bind \
        ${JVM_OPTS}"
elif [ "${version}" -ge 11 ] ; then
    JVM_OPTS="\
        --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
        --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
        --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
        --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
        --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
        --illegal-access=permit \
        ${JVM_OPTS}"
fi

DIGNITE_QUIET=$(printenv JVM_OPTS | grep -o 'IGNITE_QUIET=[^ ,]\+' | cut -d "=" -f 2)

if [ "${IGNITE_QUIET}" == "false" -o "${DIGNITE_QUIET}" == "false" ]; then
    JVM_OPTS="${JVM_OPTS} -DIGNITE_QUIET=false"
fi

#
# Start Ignite node
#
if [ -z "${CONFIG_URI}" ]; then
  exec "${JAVA}" ${JVM_OPTS} -DIGNITE_HOME="${IGNITE_HOME}" -cp "${CP}" org.apache.ignite.startup.cmdline.CommandLineStartup "${DEFAULT_CONFIG}"
else
  exec "${JAVA}" ${JVM_OPTS} -DIGNITE_HOME="${IGNITE_HOME}" -cp "${CP}" org.apache.ignite.startup.cmdline.CommandLineStartup "${CONFIG_URI}"
fi
