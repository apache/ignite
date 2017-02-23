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

import StringBuilder from './StringBuilder';
import IgniteVersion from 'app/modules/configuration/Version.service';

// Java built-in class names.
import POM_DEPENDENCIES from 'app/data/pom-dependencies.json';

/**
 * Pom file generation entry point.
 */
export default class IgniteMavenGenerator {
    escapeId(s) {
        if (typeof (s) !== 'string')
            return s;

        return s.replace(/[^A-Za-z0-9_\-.]+/g, '_');
    }

    addProperty(sb, tag, val) {
        sb.append('<' + tag + '>' + val + '</' + tag + '>');
    }

    addDependency(deps, groupId, artifactId, version, jar) {
        if (!_.find(deps, (dep) => dep.groupId === groupId && dep.artifactId === artifactId))
            deps.push({groupId, artifactId, version, jar});
    }

    addResource(sb, dir, exclude) {
        sb.startBlock('<resource>');
        if (dir)
            this.addProperty(sb, 'directory', dir);

        if (exclude) {
            sb.startBlock('<excludes>');
            this.addProperty(sb, 'exclude', exclude);
            sb.endBlock('</excludes>');
        }

        sb.endBlock('</resource>');
    }

    artifact(sb, cluster, version) {
        this.addProperty(sb, 'groupId', 'org.apache.ignite');
        this.addProperty(sb, 'artifactId', this.escapeId(cluster.name) + '-project');
        this.addProperty(sb, 'version', version);

        sb.emptyLine();
    }

    dependencies(sb, cluster, deps) {
        sb.startBlock('<dependencies>');

        _.forEach(deps, (dep) => {
            sb.startBlock('<dependency>');

            this.addProperty(sb, 'groupId', dep.groupId);
            this.addProperty(sb, 'artifactId', dep.artifactId);
            this.addProperty(sb, 'version', dep.version);

            if (dep.jar) {
                this.addProperty(sb, 'scope', 'system');
                this.addProperty(sb, 'systemPath', '${project.basedir}/jdbc-drivers/' + dep.jar);
            }

            sb.endBlock('</dependency>');
        });

        sb.endBlock('</dependencies>');

        return sb;
    }

    build(sb = new StringBuilder(), cluster, excludeGroupIds) {
        sb.startBlock('<build>');
        sb.startBlock('<resources>');
        this.addResource(sb, 'src/main/java', '**/*.java');
        this.addResource(sb, 'src/main/resources');
        sb.endBlock('</resources>');

        sb.startBlock('<plugins>');
        sb.startBlock('<plugin>');
        this.addProperty(sb, 'artifactId', 'maven-dependency-plugin');
        sb.startBlock('<executions>');
        sb.startBlock('<execution>');
        this.addProperty(sb, 'id', 'copy-libs');
        this.addProperty(sb, 'phase', 'test-compile');
        sb.startBlock('<goals>');
        this.addProperty(sb, 'goal', 'copy-dependencies');
        sb.endBlock('</goals>');
        sb.startBlock('<configuration>');
        this.addProperty(sb, 'excludeGroupIds', excludeGroupIds.join(','));
        this.addProperty(sb, 'outputDirectory', 'target/libs');
        this.addProperty(sb, 'includeScope', 'compile');
        this.addProperty(sb, 'excludeTransitive', 'true');
        sb.endBlock('</configuration>');
        sb.endBlock('</execution>');
        sb.endBlock('</executions>');
        sb.endBlock('</plugin>');
        sb.startBlock('<plugin>');
        this.addProperty(sb, 'artifactId', 'maven-compiler-plugin');
        this.addProperty(sb, 'version', '3.1');
        sb.startBlock('<configuration>');
        this.addProperty(sb, 'source', '1.7');
        this.addProperty(sb, 'target', '1.7');
        sb.endBlock('</configuration>');
        sb.endBlock('</plugin>');
        sb.endBlock('</plugins>');
        sb.endBlock('</build>');

        sb.endBlock('</project>');
    }

    /**
     * Add dependency for specified store factory if not exist.
     * @param storeDeps Already added dependencies.
     * @param storeFactory Store factory to add dependency.
     */
    storeFactoryDependency(storeDeps, storeFactory) {
        if (storeFactory.dialect && (!storeFactory.connectVia || storeFactory.connectVia === 'DataSource')) {
            const dep = POM_DEPENDENCIES[storeFactory.dialect];

            this.addDependency(storeDeps, dep.groupId, dep.artifactId, dep.version, dep.jar);
        }
    }

    /**
     * Generate pom.xml.
     *
     * @param cluster Cluster  to take info about dependencies.
     * @param version Version for Ignite dependencies.
     * @returns {string} Generated content.
     */
    generate(cluster, version = IgniteVersion.ignite) {
        const caches = cluster.caches;
        const deps = [];
        const storeDeps = [];
        const excludeGroupIds = ['org.apache.ignite'];

        const blobStoreFactory = {cacheStoreFactory: {kind: 'CacheHibernateBlobStoreFactory'}};

        _.forEach(caches, (cache) => {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind)
                this.storeFactoryDependency(storeDeps, cache.cacheStoreFactory[cache.cacheStoreFactory.kind]);

            if (_.get(cache, 'nodeFilter.kind') === 'Exclude')
                this.addDependency(deps, 'org.apache.ignite', 'ignite-extdata-p2p', version);
        });

        const sb = new StringBuilder();

        sb.append('<?xml version="1.0" encoding="UTF-8"?>');

        sb.emptyLine();

        sb.append(`<!-- ${sb.generatedBy()} -->`);

        sb.emptyLine();

        sb.startBlock('<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">');

        sb.append('<modelVersion>4.0.0</modelVersion>');

        sb.emptyLine();

        this.artifact(sb, cluster, version);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-core', version);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-spring', version);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-indexing', version);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-rest-http', version);

        if (_.get(cluster, 'deploymentSpi.kind') === 'URI')
            this.addDependency(deps, 'org.apache.ignite', 'ignite-urideploy', version);

        let dep = POM_DEPENDENCIES[cluster.discovery.kind];

        if (dep)
            this.addDependency(deps, 'org.apache.ignite', dep.artifactId, version);

        if (cluster.discovery.kind === 'Jdbc') {
            const store = cluster.discovery.Jdbc;

            if (store.dataSourceBean && store.dialect)
                this.storeFactoryDependency(storeDeps, cluster.discovery.Jdbc);
        }

        _.forEach(cluster.checkpointSpi, (spi) => {
            if (spi.kind === 'S3') {
                dep = POM_DEPENDENCIES.S3;

                if (dep)
                    this.addDependency(deps, 'org.apache.ignite', dep.artifactId, version);
            }
            else if (spi.kind === 'JDBC')
                this.storeFactoryDependency(storeDeps, spi.JDBC);
        });

        if (_.find(cluster.igfss, (igfs) => igfs.secondaryFileSystemEnabled))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hadoop', version);

        if (_.find(caches, blobStoreFactory))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hibernate', version);

        if (cluster.logger && cluster.logger.kind) {
            dep = POM_DEPENDENCIES[cluster.logger.kind];

            if (dep)
                this.addDependency(deps, 'org.apache.ignite', dep.artifactId, version);
        }

        this.dependencies(sb, cluster, deps.concat(storeDeps));

        sb.emptyLine();

        this.build(sb, cluster, excludeGroupIds);

        return sb.asString();
    }
}
