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

// Generate from
$generatorDocker.from = function(cluster, version) {
    return '# Start from Apache Ignite image.\n' +
        'FROM apacheignite/ignite:' + version
};

// Generate Docker file for cluster.
$generatorDocker.clusterDocker = function (cluster, version) {
    return  $generatorDocker.from(cluster, version) + '\n\n' +
        '# Set config uri for node.\n' +
        'ENV CONFIG_URI config/' + cluster.name + '-server.xml\n\n' +
        '# Copy ignite-http-rest from optional.\n' +
        'ENV OPTION_LIBS ignite-rest-http\n\n' +
        '# Update packages and install maven.\n' +
        'RUN \\\n' +
        '   apt-get update && \\\n' +
        '   apt-get install -y maven\n\n' +
        '# Append project to container.\n' +
        'ADD . ' + cluster.name + '\n\n' +
        '# Build project in container.\n' +
        'RUN mvn -f ' + cluster.name + '/pom.xml clean package -DskipTests\n\n' +
        '# Copy project jars to node classpath.\n' +
        'RUN mkdir $IGNITE_HOME/libs/' + cluster.name + ' && \\\n' +
        '   find ' + cluster.name + '/target -name "*.jar" -type f -exec cp {} $IGNITE_HOME/libs/' + cluster.name + ' \\; && \\\n' +
        '   cp -r ' + cluster.name + '/config/* $IGNITE_HOME/config\n'
};


$generatorDocker.ignoreFile = function() {
    return 'target\n' +
            'Dockerfile';
};
