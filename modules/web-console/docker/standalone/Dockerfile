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

FROM node:8-slim as frontend-build

ENV NPM_CONFIG_LOGLEVEL error

WORKDIR /opt/web-console

# Install node modules for frontend.
COPY frontend/package*.json frontend/
RUN (cd frontend && npm install --no-optional)

# Copy source.
COPY frontend frontend

RUN (cd frontend && npm run build)

FROM node:8-slim

ENV NPM_CONFIG_LOGLEVEL error

# Update package list & install.
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6 \
    && echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/3.4 main" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list

# Update package list & install.
RUN apt-get update \
    && apt-get install -y nginx-light mongodb-org-server dos2unix \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install global node packages.
RUN npm install -g pm2

WORKDIR /opt/web-console

COPY docker/standalone/docker-entrypoint.sh docker-entrypoint.sh

RUN chmod +x docker-entrypoint.sh \
    && dos2unix docker-entrypoint.sh

# Copy nginx config.
COPY docker/standalone/nginx/* /etc/nginx/

# Install node modules for frontend and backend modules.
COPY backend/package*.json backend/
RUN (cd backend && npm install --no-optional --production)

# Copy source.
COPY backend backend

COPY web-agent/target/ignite-web-agent-*.zip backend/agent_dists

COPY --from=frontend-build /opt/web-console/frontend/build static

VOLUME ["/etc/nginx"]
VOLUME ["/data/db"]
VOLUME ["/opt/web-console/serve/agent_dists"]

EXPOSE 80
ENTRYPOINT ["/opt/web-console/docker-entrypoint.sh"]
