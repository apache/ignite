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

# Start from basic Alpine Linux image (~5Mb)
FROM alpine:3.7

# Set Ignite version
ENV IGNITE_VERSION 2.3.0
# Set Ignite home directory
ENV IGNITE_HOME /opt/ignite/apache-ignite-fabric-${IGNITE_VERSION}-bin

# Set docker image metadata
LABEL maintainer="Peter Ivanov <mr.weider@gmail.com>"

# Add missing software
RUN apk --update add \
	openjdk8 \
	curl \
	bash \
	gnupg

# Set working directory
WORKDIR /opt/ignite

# Get and unarchive Ignite (checking the signatures on the way)
RUN set -x \
    && set -o errexit \
    && curl -fSL "https://archive.apache.org/dist/ignite/${IGNITE_VERSION}/apache-ignite-fabric-${IGNITE_VERSION}-bin.zip" -o ignite.zip \
    && curl -fSL "https://archive.apache.org/dist/ignite/${IGNITE_VERSION}/apache-ignite-fabric-${IGNITE_VERSION}-bin.zip.asc" -o ignite.zip.asc \
    && export GNUPGHOME="$(mktemp -d)" \
    && curl -fSl "https://archive.apache.org/dist/ignite/KEYS.txt" -o KEYS.txt \
    && gpg --import KEYS.txt \
    && gpg --batch --verify ignite.zip.asc ignite.zip \
    && unzip ignite.zip \
    && rm -rfv "$GNUPGHOME" ignite.zip* KEYS.txt \
    && unset GNUPGHOME


# Copy entrypoint executable
COPY ./run.sh $IGNITE_HOME/

# Entrypoint
CMD "$IGNITE_HOME/run.sh"

# Open required ports
EXPOSE 11211 47100 47500 49112

