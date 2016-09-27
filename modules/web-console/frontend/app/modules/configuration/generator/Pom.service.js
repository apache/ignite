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

// Java built-in class names.
import POM_DEPENDENCIES from 'app/data/pom-dependencies.json';

/**
 * Pom file generation entry point.
 */
class GeneratorPom {
    escapeId(s) {
        if (typeof (s) !== 'string')
            return s;

        return s.replace(/[^A-Za-z0-9_\-.]+/g, '_');
    }

    addProperty(res, tag, val) {
        res.line('<' + tag + '>' + val + '</' + tag + '>');
    }

    addDependency(deps, groupId, artifactId, version, jar) {
        if (!_.find(deps, (dep) => dep.groupId === groupId && dep.artifactId === artifactId))
            deps.push({groupId, artifactId, version, jar});
    }

    addResource(res, dir, exclude) {
        res.startBlock('<resource>');
        if (dir)
            this.addProperty(res, 'directory', dir);

        if (exclude) {
            res.startBlock('<excludes>');
            this.addProperty(res, 'exclude', exclude);
            res.endBlock('</excludes>');
        }

        res.endBlock('</resource>');
    }

    artifact(res, cluster, version) {
        this.addProperty(res, 'groupId', 'org.apache.ignite');
        this.addProperty(res, 'artifactId', this.escapeId(cluster.name) + '-project');
        this.addProperty(res, 'version', version);

        res.needEmptyLine = true;
    }

    dependencies(res, cluster, deps) {
        if (!res)
            res = $generatorCommon.builder();

        res.startBlock('<dependencies>');

        _.forEach(deps, (dep) => {
            res.startBlock('<dependency>');

            this.addProperty(res, 'groupId', dep.groupId);
            this.addProperty(res, 'artifactId', dep.artifactId);
            this.addProperty(res, 'version', dep.version);

            if (dep.jar) {
                this.addProperty(res, 'scope', 'system');
                this.addProperty(res, 'systemPath', '${project.basedir}/jdbc-drivers/' + dep.jar);
            }

            res.endBlock('</dependency>');
        });

        res.endBlock('</dependencies>');

        return res;
    }

    build(res, cluster, excludeGroupIds) {
        res.startBlock('<build>');
        res.startBlock('<resources>');
        this.addResource(res, 'src/main/java', '**/*.java');
        this.addResource(res, 'src/main/resources');
        res.endBlock('</resources>');

        res.startBlock('<plugins>');
        res.startBlock('<plugin>');
        this.addProperty(res, 'artifactId', 'maven-dependency-plugin');
        res.startBlock('<executions>');
        res.startBlock('<execution>');
        this.addProperty(res, 'id', 'copy-libs');
        this.addProperty(res, 'phase', 'test-compile');
        res.startBlock('<goals>');
        this.addProperty(res, 'goal', 'copy-dependencies');
        res.endBlock('</goals>');
        res.startBlock('<configuration>');
        this.addProperty(res, 'excludeGroupIds', excludeGroupIds.join(','));
        this.addProperty(res, 'outputDirectory', 'target/libs');
        this.addProperty(res, 'includeScope', 'compile');
        this.addProperty(res, 'excludeTransitive', 'true');
        res.endBlock('</configuration>');
        res.endBlock('</execution>');
        res.endBlock('</executions>');
        res.endBlock('</plugin>');
        res.startBlock('<plugin>');
        this.addProperty(res, 'artifactId', 'maven-compiler-plugin');
        this.addProperty(res, 'version', '3.1');
        res.startBlock('<configuration>');
        this.addProperty(res, 'source', '1.7');
        this.addProperty(res, 'target', '1.7');
        res.endBlock('</configuration>');
        res.endBlock('</plugin>');
        res.endBlock('</plugins>');
        res.endBlock('</build>');

        res.endBlock('</project>');
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
     * @param version Ignite version for Ignite dependencies.
     * @param res Resulting output with generated pom.
     * @returns {string} Generated content.
     */
    generate(cluster, version, res) {
        const caches = cluster.caches;
        const deps = [];
        const storeDeps = [];
        const excludeGroupIds = ['org.apache.ignite'];

        const blobStoreFactory = {cacheStoreFactory: {kind: 'CacheHibernateBlobStoreFactory'}};

        if (!res)
            res = $generatorCommon.builder();

        _.forEach(caches, (cache) => {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind)
                this.storeFactoryDependency(storeDeps, cache.cacheStoreFactory[cache.cacheStoreFactory.kind]);

            if (_.get(cache, 'nodeFilter.kind') === 'Exclude')
                this.addDependency(deps, 'org.apache.ignite', 'ignite-extdata-p2p', version);
        });

        res.line('<?xml version="1.0" encoding="UTF-8"?>');

        res.needEmptyLine = true;

        res.line('<!-- ' + $generatorCommon.mainComment('Maven project') + ' -->');

        res.needEmptyLine = true;

        res.startBlock('<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">');

        res.line('<modelVersion>4.0.0</modelVersion>');

        res.needEmptyLine = true;

        this.artifact(res, cluster, version);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-core', version);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-spring', version);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-indexing', version);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-rest-http', version);

        let dep = POM_DEPENDENCIES[cluster.discovery.kind];

        if (dep)
            this.addDependency(deps, 'org.apache.ignite', dep.artifactId, version);

        if (cluster.discovery.kind === 'Jdbc') {
            const store = cluster.discovery.Jdbc;

            if (store.dataSourceBean && store.dialect)
                this.storeFactoryDependency(storeDeps, cluster.discovery.Jdbc);
        }

        if (_.find(cluster.igfss, (igfs) => igfs.secondaryFileSystemEnabled))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hadoop', version);

        if (_.find(caches, blobStoreFactory))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hibernate', version);

        if (cluster.logger && cluster.logger.kind) {
            dep = POM_DEPENDENCIES[cluster.logger.kind];

            if (dep)
                this.addDependency(deps, 'org.apache.ignite', dep.artifactId, version);
        }

        this.dependencies(res, cluster, deps.concat(storeDeps));

        res.needEmptyLine = true;

        this.build(res, cluster, excludeGroupIds);

        return res;
    }
}

export default ['GeneratorPom', GeneratorPom];
