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
# Start from Java 8 based on Alpine Linux image (~5Mb)
FROM openjdk:8-jre-alpine

# Settings
ENV IGNITE_HOME /opt/ignite/apache-ignite
WORKDIR /opt/ignite

# Add missing software
RUN apk --no-cache \
    add bash

# Copy main binary archive
COPY apache-ignite* apache-ignite

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

# Container port exposure
EXPOSE 11211 47100 47500 49112 10800 8080
