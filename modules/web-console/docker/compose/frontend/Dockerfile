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

FROM nginx:1-alpine

WORKDIR /data/www

COPY --from=frontend-build /opt/web-console/frontend/build .

COPY docker/compose/frontend/nginx/nginx.conf /etc/nginx/nginx.conf
COPY docker/compose/frontend/nginx/web-console.conf /etc/nginx/web-console.conf

VOLUME /etc/nginx
VOLUME /data/www

EXPOSE 80
