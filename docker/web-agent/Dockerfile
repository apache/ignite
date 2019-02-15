#
#                   GridGain Community Edition Licensing
#                   Copyright 2019 GridGain Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
# Restriction; you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
# Commons Clause Restriction
#
# The Software is provided to you by the Licensor under the License, as defined below, subject to
# the following condition.
#
# Without limiting other conditions in the License, the grant of rights under the License will not
# include, and the License does not grant to you, the right to Sell the Software.
# For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
# under the License to provide to third parties, for a fee or other consideration (including without
# limitation fees for hosting or consulting/ support services related to the Software), a product or
# service whose value derives, entirely or substantially, from the functionality of the Software.
# Any license notice or attribution required by the License must also include this Commons Clause
# License Condition notice.
#
# For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
# the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
# Edition software provided with this notice.
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

