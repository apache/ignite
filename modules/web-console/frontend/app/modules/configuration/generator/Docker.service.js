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

/**
 * Docker file generation entry point.
 */
export default class IgniteDockerGenerator {
    /**
     * Generate from section.
     *
     * @param {Object} cluster Cluster.
     * @param {String} ver Ignite version.
     * @returns {String}
     */
    from(cluster, ver) {
        return [
            '# Start from Apache Ignite image.',
            `FROM apacheignite/ignite:${ver}`
        ].join('\n');
    }

    /**
     * Generate Docker file for cluster.
     *
     * @param {Object} cluster Cluster.
     * @param {String} ver Ignite version.
     */
    generate(cluster, ver) {
        return [
            this.from(cluster, ver),
            '',
            '# Set config uri for node.',
            `ENV CONFIG_URI config/${cluster.name}-server.xml`,
            '',
            '# Copy ignite-http-rest from optional.',
            'ENV OPTION_LIBS ignite-rest-http',
            '',
            '# Update packages and install maven.',
            'RUN \\',
            '   apt-get update &&\\',
            '   apt-get install -y maven',
            '',
            '# Append project to container.',
            `ADD . ${cluster.name}`,
            '',
            '# Build project in container.',
            `RUN mvn -f ${cluster.name}/pom.xml clean package -DskipTests`,
            '',
            '# Copy project jars to node classpath.',
            `RUN mkdir $IGNITE_HOME/libs/${cluster.name} && \\`,
            `   find ${cluster.name}/target -name "*.jar" -type f -exec cp {} $IGNITE_HOME/libs/${cluster.name} \\; && \\`,
            `   cp -r ${cluster.name}/config/* $IGNITE_HOME/config`
        ].join('\n');
    }

    ignoreFile() {
        return [
            'target',
            'Dockerfile'
        ].join('\n');
    }
}
