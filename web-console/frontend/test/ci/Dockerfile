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
FROM node:8-alpine

ENV NPM_CONFIG_LOGLEVEL error

RUN apk --no-cache --repository http://dl-3.alpinelinux.org/alpine/edge/testing/ add \
 chromium xwininfo xvfb dbus eudev ttf-freefont fluxbox

ENV CHROME_BIN /usr/bin/chromium-browser

WORKDIR /opt/web-console/frontend

COPY ./package*.json ./
RUN npm install

COPY . .

ENTRYPOINT ["npm", "test"]
