#!/bin/sh
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

# Extract java version to `version` variable.
javaVersion() {
    version=$("$1" -version 2>&1 | awk -F[\"\-] '/version/ {print $2}')
}

# Extract only major version of java to `version` variable.
javaMajorVersion() {
    javaVersion "$1"
    version="${version%%.*}"

    if [ ${version} -eq 1 ]; then
        # Version seems starts from 1, we need second number.
        javaVersion "$1"
        version=$(awk -F[\"\.] '{print $2}' <<< ${version})
    fi
}

#
# Discovers path to Java executable and checks it's version.
# The function exports JAVA variable with path to Java executable.
#
checkJava() {
    # Check JAVA_HOME.
    if [ "${JAVA_HOME:-}" = "" ]; then
        JAVA=`type -p java`
        RETCODE=$?

        if [ $RETCODE -ne 0 ]; then
            echo $0", ERROR:"
            echo "JAVA_HOME environment variable is not found."
            echo "Please point JAVA_HOME variable to location of JDK 1.8 or later."
            echo "You can also download latest JDK at http://java.com/download"

            exit 1
        fi

        JAVA_HOME=
    else
        JAVA=${JAVA_HOME}/bin/java
    fi

    #
    # Check JDK.
    #
    javaMajorVersion "$JAVA"

    if [ $version -lt 8 ]; then
        echo "$0, ERROR:"
        echo "The $version version of JAVA installed in JAVA_HOME=$JAVA_HOME is incompatible."
        echo "Please point JAVA_HOME variable to installation of JDK 1.8 or later."
        echo "You can also download latest JDK at http://java.com/download"
        exit 1
    fi
}

# Gets java specific options like add-exports and add-opens
# First argument is the version of the java
# Second argument is the current value of the jvm options
getJavaSpecificOpts() {
  version=$1
  current_value=$2
  value=""

  if [ $version -eq 8 ] ; then
      value="\
          -XX:+AggressiveOpts \
           ${current_value}"

  elif [ $version -gt 8 ] && [ $version -lt 11 ]; then
      value="\
          -XX:+AggressiveOpts \
          --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --illegal-access=permit \
          --add-modules=java.xml.bind \
          ${current_value}"

  elif [ "${version}" -ge 11 ] && [ "${version}" -lt 14 ]; then
      value="\
          --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
          --illegal-access=permit \
          ${current_value}"

  elif [ "${version}" -ge 14 ] && [ "${version}" -lt 15 ]; then
        value="\
            --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
            --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
            --add-exports=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
            --add-exports=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
            --add-exports=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
            --add-opens=java.base/jdk.internal.access=ALL-UNNAMED \
            --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
            --illegal-access=permit \
            ${current_value}"

  elif [ "${version}" -ge 15 ] ; then
      value="\
          --add-opens=java.base/jdk.internal.access=ALL-UNNAMED \
          --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
          --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
          --add-opens=java.management/com.sun.jmx.mbeanserver=ALL-UNNAMED \
          --add-opens=jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED \
          --add-opens=java.base/sun.reflect.generics.reflectiveObjects=ALL-UNNAMED \
          --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
          --add-opens=java.base/java.io=ALL-UNNAMED \
          --add-opens=java.base/java.nio=ALL-UNNAMED \
          --add-opens=java.base/java.net=ALL-UNNAMED \
          --add-opens=java.base/java.util=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
          --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
          --add-opens=java.base/java.lang=ALL-UNNAMED \
          --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
          --add-opens=java.base/java.math=ALL-UNNAMED \
          --add-opens=java.sql/java.sql=ALL-UNNAMED \
          --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
          --add-opens=java.base/java.time=ALL-UNNAMED \
          --add-opens=java.base/java.text=ALL-UNNAMED \
          --add-opens=java.management/sun.management=ALL-UNNAMED \
          --add-opens java.desktop/java.awt.font=ALL-UNNAMED \
          ${current_value}"
  fi

  echo $value
}
