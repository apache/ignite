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

# Extend jre image
FROM openjdk:11-jre
# Set the location of the application
ENV WEB_CONSOLE_HOME /opt/ignite-web-console-server

WORKDIR $WEB_CONSOLE_HOME

# Copy your verticle to the container
COPY ./ignite-web-console-*/*.jar ./
COPY ./ignite-web-console-*/web-console.sh ./
COPY ./ignite-web-console-*/libs ./libs
COPY ./ignite-web-console-*/agent_dists ./agent_dists

EXPOSE 3000

CMD ./web-console.sh
