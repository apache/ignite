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
FROM  s390x/ubuntu:20.04

# Dockerfile variables
ARG IGNITE_VER

# Set Environment Variables
ENV JAVA_HOME="/opt/jdk-11.0.14.1+1"
ENV IGNITE_HOME="/opt/apache-ignite-${IGNITE_VER}-bin"
ENV PATH="${JAVA_HOME}/bin:/opt/apache-ignite-${IGNITE_VER}-bin/bin:${PATH}"
ENV JDK_FILE="ibm-semeru-open-jdk_s390x_linux_11.0.14.1_1_openj9-0.30.1.tar.gz"

# The Author
LABEL maintainer="LoZ Open Source Ecosystem (https://www.ibm.com/community/z/usergroups/opensource)"


# Main directory
WORKDIR /opt 

# Install dependencies
RUN apt-get update && \
    apt-get install -y \
        wget \
        tar \
        unzip && \
    apt-get -y clean autoremove && \
    rm -rf /var/lib/apt/lists/*

# Download JDK 11
RUN wget https://github.com/ibmruntimes/semeru11-binaries/releases/download/jdk-11.0.14.1%2B1_openj9-0.30.1/${JDK_FILE} && \
    tar xvf ${JDK_FILE} && \
    rm -rf ${JDK_FILE}

# Install Apache Ignite
RUN wget https://archive.apache.org/dist/ignite/${IGNITE_VER}/apache-ignite-${IGNITE_VER}-bin.zip && \
    unzip -q apache-ignite-${IGNITE_VER}-bin.zip && \
    rm -rfv apache-ignite-${IGNITE_VER}-bin.zip

# Copy sh files and set permission
COPY run.sh ${IGNITE_HOME}/

# Grant permission to copy optional libs
RUN chmod -R 777 ${IGNITE_HOME}/libs ${IGNITE_HOME}

# Grant permission to execute entry point
RUN chmod 555 ${IGNITE_HOME}/run.sh

# Entry point
CMD ${IGNITE_HOME}/run.sh

# Exposing the ports.
EXPOSE 11211 47100 47500 49112
