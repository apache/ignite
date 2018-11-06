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

import {outdent} from 'outdent/lib';
import VersionService from 'app/services/Version.service';
import POM_DEPENDENCIES from 'app/data/pom-dependencies.json';
import get from 'lodash/get';

const version = new VersionService();

const ALPINE_DOCKER_SINCE = '2.1.0';

/**
 * Docker file generation entry point.
 */
export default class IgniteDockerGenerator {
    escapeFileName = (name) => name.replace(/[\\\/*\"\[\],\.:;|=<>?]/g, '-').replace(/ /g, '_');

    /**
     * Generate from section.
     *
     * @param {Object} cluster Cluster.
     * @param {Object} targetVer Target version.
     * @returns {String}
     */
    from(cluster, targetVer) {
        return outdent`
            # Start from Apache Ignite image.',
            FROM apacheignite/ignite:${targetVer.ignite}
        `;
    }

    /**
     * Generate Docker file for cluster.
     *
     * @param {Object} cluster Cluster.
     * @param {Object} targetVer Target version.
     */
    generate(cluster, targetVer) {
        return outdent`
            ${this.from(cluster, targetVer)}

            # Set config uri for node.
            ENV CONFIG_URI ${this.escapeFileName(cluster.name)}-server.xml

            # Copy optional libs.
            ENV OPTION_LIBS ${this.optionLibs(cluster, targetVer).join(',')}

            # Update packages and install maven.
            ${this.packages(cluster, targetVer)}
            
            # Append project to container.
            ADD . ${cluster.name}

            # Build project in container.
            RUN mvn -f ${cluster.name}/pom.xml clean package -DskipTests

            # Copy project jars to node classpath.
            RUN mkdir $IGNITE_HOME/libs/${cluster.name} && \\
               find ${cluster.name}/target -name "*.jar" -type f -exec cp {} $IGNITE_HOME/libs/${cluster.name} \\;
        `;
    }

    optionLibs(cluster, targetVer) {
        return [
            'ignite-rest-http',
            get(POM_DEPENDENCIES, [get(cluster, 'discovery.kind'), 'artifactId'])
        ].filter(Boolean);
    }

    packages(cluster, targetVer) {
        return version.since(targetVer.ignite, ALPINE_DOCKER_SINCE)
            ? outdent`
                RUN set -x \\
                    && apk add --no-cache \\
                        openjdk8

                RUN apk --update add \\
                    maven \\
                    && rm -rfv /var/cache/apk/*
            `
            : outdent`
                RUN \\
                   apt-get update &&\\
                   apt-get install -y maven
            `;
    }

    ignoreFile() {
        return outdent`
            target
            Dockerfile
        `;
    }
}
