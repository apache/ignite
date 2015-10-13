/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Docker file generation entry point.
$generatorDocker = {};

// Generate Docker file for cluster.
$generatorDocker.clusterDocker = function (cluster, os) {
    if (!os)
        os = 'debian:8';

    return '# Start from a OS image.\n' +
        'FROM ' + os + '\n' +
        '\n' +
        '# Install tools.\n' +
        'RUN apt-get update && apt-get install -y --fix-missing \\\n' +
        '  wget \\\n' +
        '  dstat \\\n' +
        '  maven \\\n' +
        '  git\n' +
        '\n' +
        '# Install Java. \n' +
        'RUN \\\n' +
        'apt-get update && \\\n' +
        'apt-get install -y openjdk-7-jdk && \\\n' +
        'rm -rf /var/lib/apt/lists/*\n' +
        '\n' +
        '# Define commonly used JAVA_HOME variable.\n' +
        'ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64\n' +
        '\n' +
        '# Create working directory\n' +
        'WORKDIR /home\n' +
        '\n' +
        'RUN wget -O ignite.zip http://tiny.cc/updater/download_ignite.php && unzip ignite.zip && rm ignite.zip\n' +
        '\n' +
        'COPY *.xml /tmp/\n' +
        '\n' +
        'RUN mv /tmp/*.xml /home/$(ls)/config';
};

// For server side we should export Java code generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorDocker;
}
