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

#~~~~~~~~~~~~~~~~~~#
#  Frontend build  #
#~~~~~~~~~~~~~~~~~~#
FROM node:10-stretch as frontend-build

ENV NPM_CONFIG_LOGLEVEL error

WORKDIR /opt/web-console

# Install node modules and build sources
COPY frontend frontend
RUN cd frontend && \
    npm install --no-optional && \
    npm run build


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#  Web Console Standalone assemble  #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
FROM node:10-stretch

ENV NPM_CONFIG_LOGLEVEL error

# Install global node packages
RUN npm install -g pm2

# Update software sources and install missing applications
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 9DA31620334BD75D9DCB49F368818C72E52529D4 \
    && echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/4.0 main" | tee /etc/apt/sources.list.d/mongodb-org-4.0.list
    apt update && \
    apt install -y --no-install-recommends \
        nginx-light \
        mongodb-org-server \
        dos2unix && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/web-console

# Install node modules for backend
COPY backend/package*.json backend/
RUN cd backend && \
    npm install --no-optional --production

# Copy and build sources
COPY backend backend
RUN cd backend && \
    npm run build

# Copy Ignite Web Agent module package
COPY ignite-web-agent-*.zip backend/agent_dists

# Copy previously built frontend
COPY --from=frontend-build /opt/web-console/frontend/build static

# Copy and fix entrypoint script
COPY docker-entrypoint.sh docker-entrypoint.sh
RUN chmod +x docker-entrypoint.sh \
    && dos2unix docker-entrypoint.sh

# Copy nginx configuration
COPY nginx/* /etc/nginx/

EXPOSE 80

ENTRYPOINT ["/opt/web-console/docker-entrypoint.sh"]

