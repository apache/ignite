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

    artifact(res, cluster, igniteVersion) {
        this.addProperty(res, 'groupId', 'org.apache.ignite');
        this.addProperty(res, 'artifactId', this.escapeId(cluster.name) + '-project');
        this.addProperty(res, 'version', igniteVersion);

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
     * Generate pom.xml.
     *
     * @param cluster Cluster  to take info about dependencies.
     * @param igniteVersion Ignite version for Ignite dependencies.
     * @param res Resulting output with generated pom.
     * @returns {string} Generated content.
     */
    generate(cluster, igniteVersion, res) {
        const caches = cluster.caches;
        const dialect = {};
        const deps = [];
        const excludeGroupIds = ['org.apache.ignite'];

        const blobStoreFactory = {cacheStoreFactory: {kind: 'CacheHibernateBlobStoreFactory'}};

        if (!res)
            res = $generatorCommon.builder();

        _.forEach(caches, (cache) => {
            if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind) {
                const storeFactory = cache.cacheStoreFactory[cache.cacheStoreFactory.kind];

                if (storeFactory.dialect && (!storeFactory.connectVia || storeFactory.connectVia === 'DataSource'))
                    dialect[storeFactory.dialect] = true;
            }
        });

        res.line('<?xml version="1.0" encoding="UTF-8"?>');

        res.needEmptyLine = true;

        res.line('<!-- ' + $generatorCommon.mainComment() + ' -->');

        res.needEmptyLine = true;

        res.startBlock('<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">');

        res.line('<modelVersion>4.0.0</modelVersion>');

        res.needEmptyLine = true;

        this.artifact(res, cluster, igniteVersion);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-core', igniteVersion);

        this.addDependency(deps, 'org.apache.ignite', 'ignite-spring', igniteVersion);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-indexing', igniteVersion);
        this.addDependency(deps, 'org.apache.ignite', 'ignite-rest-http', igniteVersion);

        if (cluster.discovery.kind === 'Cloud')
            this.addDependency(deps, 'org.apache.ignite', 'ignite-cloud', igniteVersion);
        else if (cluster.discovery.kind === 'S3')
            this.addDependency(deps, 'org.apache.ignite', 'ignite-aws', igniteVersion);
        else if (cluster.discovery.kind === 'GoogleStorage')
            this.addDependency(deps, 'org.apache.ignite', 'ignite-gce', igniteVersion);
        else if (cluster.discovery.kind === 'ZooKeeper')
            this.addDependency(deps, 'org.apache.ignite', 'ignite-zookeeper', igniteVersion);

        if (_.find(cluster.igfss, (igfs) => igfs.secondaryFileSystemEnabled))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hadoop', igniteVersion);

        if (_.find(caches, blobStoreFactory))
            this.addDependency(deps, 'org.apache.ignite', 'ignite-hibernate', igniteVersion);

        dialect.Generic && this.addDependency(deps, 'com.mchange', 'c3p0', '0.9.5.1');

        dialect.MySQL && this.addDependency(deps, 'mysql', 'mysql-connector-java', '5.1.37');

        dialect.PostgreSQL && this.addDependency(deps, 'org.postgresql', 'postgresql', '9.4-1204-jdbc42');

        dialect.H2 && this.addDependency(deps, 'com.h2database', 'h2', '1.3.175');

        dialect.Oracle && this.addDependency(deps, 'oracle', 'jdbc', '11.2', 'ojdbc6.jar');

        dialect.DB2 && this.addDependency(deps, 'ibm', 'jdbc', '4.19.26', 'db2jcc4.jar');

        dialect.SQLServer && this.addDependency(deps, 'microsoft', 'jdbc', '4.1', 'sqljdbc41.jar');

        this.dependencies(res, cluster, deps);

        res.needEmptyLine = true;

        this.build(res, cluster, excludeGroupIds);

        return res;
    }
}

export default ['GeneratorPom', GeneratorPom];
