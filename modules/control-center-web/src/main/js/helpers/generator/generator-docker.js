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
    return '# Start from apache ignite image.\n' +
        'FROM apacheignite/ignite:' + version
};

// Generate secret properties if needed.
$generatorDocker.secret = function(cluster) {
    if ($generatorCommon.secretPropertiesNeeded(cluster))
        return '# Append secret.properties file to container.\n' +
            'ADD ./src/main/resources/secret.properties $IGNITE_HOME/config/secret.properties\n\n' +
            '# Add secret.properties file to classpath.\n' +
            'ENV USER_LIBS $IGNITE_HOME/config';

    return '';
};

// Generate Docker file for cluster.
$generatorDocker.clusterDocker = function (cluster, version) {
    return  $generatorDocker.from(cluster, version) + '\n\n' +
        '# Set config uri for node.\n' +
        'ENV CONFIG_URI config/' + cluster.name + '-server.xml\n\n' +
        '# Copy ignite-http-rest from optional.\n' +
        'ENV OPTION_LIBS ignite-rest-http\n\n' +
        '# Append config file to container.\n' +
        'ADD ./config $IGNITE_HOME/config\n\n' +
        $generatorDocker.secret(cluster) + '\n';
};
