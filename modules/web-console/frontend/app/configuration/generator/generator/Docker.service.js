/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
