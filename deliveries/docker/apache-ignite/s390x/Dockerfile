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


# Base Image
FROM  s390x/ubuntu:18.04

ARG IGNITE_VER=2.9.1

# The Author
LABEL maintainer="LoZ Open Source Ecosystem (https://www.ibm.com/community/z/usergroups/opensource)"

# Set Environment Variables
ENV WORKDIR /root
ENV JAVA_HOME=$WORKDIR/jdk-11.0.7+10
ENV IGNITE_HOME=$WORKDIR/apache-ignite-${IGNITE_VER}-bin
ENV PATH=$JAVA_HOME/bin:$WORKDIR/apache-ignite-${IGNITE_VER}-bin/bin:$PATH

# Install Dependencies
RUN apt-get update &&  apt-get install -y \
        wget \
        tar \
        unzip \
# Download AdoptOpenJDK 8
 && cd $WORKDIR  \
 && wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.7%2B10_openj9-0.20.0/OpenJDK11U-jdk_s390x_linux_openj9_11.0.7_10_openj9-0.20.0.tar.gz \
 && tar xvf OpenJDK11U-jdk_s390x_linux_openj9_11.0.7_10_openj9-0.20.0.tar.gz \
# Install Apache Ignite
 && cd $WORKDIR  \
 && wget https://archive.apache.org/dist/ignite/${IGNITE_VER}/apache-ignite-${IGNITE_VER}-bin.zip \
 && unzip -q apache-ignite-${IGNITE_VER}-bin.zip  \
# Clean up cache data and remove dependencies that are not required
 && apt-get remove -y \
    wget \
    unzip \
 && apt-get autoremove -y \
 && apt autoremove -y \
 && apt-get clean && rm -rf /var/lib/apt/lists/*  $WORKDIR/apache-ignite-${IGNITE_VER}-bin.zip $WORKDIR/OpenJDK11U-jdk_s390x_linux_openj9_11.0.7_10_openj9-0.20.0.tar.gz

# Copy sh files and set permission
COPY run.sh $IGNITE_HOME/
# Grant permission to copy optional libs
RUN chmod 777 ${IGNITE_HOME}/libs
# Grant permission to create work directory
RUN chmod 777 ${IGNITE_HOME}
# Grant permission to execute entry point
RUN chmod 555 $IGNITE_HOME/run.sh
# Entry point
CMD $IGNITE_HOME/run.sh

# Exposing the ports.
EXPOSE 11211 47100 47500 49112

# Start the Apache Ignite server
CMD ["ignite.sh"]
