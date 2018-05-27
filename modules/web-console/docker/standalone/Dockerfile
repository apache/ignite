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

FROM ubuntu:14.04

ENV NPM_CONFIG_LOGLEVEL info
ENV NODE_VERSION 8.11.1

RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927 && \
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list

# Update package list & install.
RUN apt-get update && \
    apt-get install -y nginx-light mongodb-org-server curl xz-utils git

# Install Node JS.
RUN curl -SLO "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-linux-x64.tar.xz" && \
    curl -SLO "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt" && \
    grep " node-v$NODE_VERSION-linux-x64.tar.xz\$" SHASUMS256.txt | sha256sum -c - && \
    tar -xJf "node-v$NODE_VERSION-linux-x64.tar.xz" -C /usr/local --strip-components=1 && \
    rm -rf "node-v$NODE_VERSION-linux-x64.tar.xz" SHASUMS256.txt

# Install global node packages.
RUN npm install -g pm2

# Install frontend & backend apps.
RUN mkdir -p /opt/web-console

# Copy source.
WORKDIR /opt/web-console
COPY build .

# Install node modules for frontend and backend modules.
RUN cd /opt/web-console/frontend && \
    npm install --no-optional --prod && \
    npm run build && \
    cd /opt/web-console/backend && \
    npm install --no-optional --prod

# Copy nginx config.
COPY nginx/* /etc/nginx/

# Copy entrypoint.
COPY entrypoint.sh .

# Clean up.
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

VOLUME ["/etc/nginx"]
VOLUME ["/var/lib/mongodb"]
VOLUME ["/opt/web-console/serve/agent_dists"]

EXPOSE 80

ENTRYPOINT ["/opt/web-console/entrypoint.sh"]

