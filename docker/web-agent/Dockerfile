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

# Start from Java 8 based on Alpine Linux image (~5Mb)
FROM openjdk:8-jre-alpine

# Provide default arguments
ARG DEFAULT_DRIVER_FOLDER="/opt/ignite/drivers"
ARG DEFAULT_NODE_URI="http://localhost:8080"
ARG DEFAULT_SERVER_URI="http://localhost"
ARG DEFAULT_TOKENS="NO_TOKENS"

ENV DRIVER_FOLDER=$DEFAULT_DRIVER_FOLDER
ENV NODE_URI=$DEFAULT_NODE_URI
ENV SERVER_URI=$DEFAULT_SERVER_URI
ENV TOKENS=$DEFAULT_TOKENS

# Settings
USER root
ENV AGENT_HOME /opt/ignite/ignite-web-agent
WORKDIR ${AGENT_HOME} 

# Add missing software
RUN apk --no-cache \
    add bash

# Copy main binary archive
COPY ignite-web-agent*/* ./

# Entrypoint
CMD ./ignite-web-agent.sh -d ${DRIVER_FOLDER} -n ${NODE_URI} -s ${SERVER_URI} -t ${TOKENS}

