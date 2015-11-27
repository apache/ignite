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

// pom.xml generation entry point.
$generatorPom = {};

$generatorPom.addProperty = function (res, tag, val) {
    res.line('<' + tag + '>' + val + '</' + tag + '>');
};

$generatorPom.dependency = function (groupId, artifactId, version, jar) {
    return {
        groupId: groupId,
        artifactId: artifactId,
        version: version,
        jar: jar
    };
};

$generatorPom.dependencies = function (res, cluster, deps) {
    if (!res)
        res = $generatorCommon.builder();

    res.startBlock('<dependencies>');

    _.forEach(deps, function (dep) {
        res.startBlock('<dependency>');

        $generatorPom.addProperty(res, 'groupId', dep.groupId);
        $generatorPom.addProperty(res, 'artifactId', dep.artifactId);
        $generatorPom.addProperty(res, 'version', dep.version);

        if (dep.jar) {
            $generatorPom.addProperty(res, 'scope', 'system');
            $generatorPom.addProperty(res, 'systemPath', '${project.basedir}/jdbc-drivers/' + dep.jar);
        }

        res.endBlock('</dependency>');
    });

    res.endBlock('</dependencies>');

    return res;
};

/**
 * Generate pom.xml.
 *
 * @param cluster Cluster  to take info about dependencies.
 * @param igniteVersion Ignite version for Ignite dependencies.
 * @param res Resulting output with generated pom.
 * @returns {string} Generated content.
 */
$generatorPom.pom = function (cluster, igniteVersion, res) {
    if (!res)
        res = $generatorCommon.builder();

    var caches = cluster.caches;

    var dialect = {};

    _.forEach(caches, function (cache) {
        if (cache.cacheStoreFactory && cache.cacheStoreFactory.kind == 'CacheJdbcPojoStoreFactory') {
            if (cache.cacheStoreFactory.CacheJdbcPojoStoreFactory) {
                dialect[cache.cacheStoreFactory.CacheJdbcPojoStoreFactory.dialect] = true;
            }
        }
    });

    var dependencies = [];

    function addDependency(groupId, artifactId, version, jar) {
        dependencies.push({
            groupId: groupId,
            artifactId: artifactId,
            version: version,
            jar: jar
        });
    }

    function addResource(dir, exclude) {
        res.startBlock('<resource>');
        if (dir)
            $generatorPom.addProperty(res, 'directory', dir);

        if (exclude) {
            res.startBlock('<excludes>');
            $generatorPom.addProperty(res, 'exclude', exclude);
            res.endBlock('</excludes>');
        }

        res.endBlock('</resource>');
    }

    res.line('<?xml version="1.0" encoding="UTF-8"?>');

    res.needEmptyLine = true;

    res.line('<!-- ' + $generatorCommon.mainComment() + ' -->');

    res.needEmptyLine = true;

    res.startBlock('<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">');

    res.line('<modelVersion>4.0.0</modelVersion>');

    res.needEmptyLine = true;

    $generatorPom.addProperty(res, 'groupId', 'org.apache.ignite');
    $generatorPom.addProperty(res, 'artifactId', 'ignite-generated-model');
    $generatorPom.addProperty(res, 'version', igniteVersion);

    res.needEmptyLine = true;

    res.startBlock('<repositories>');
    res.startBlock('<repository>');
    $generatorPom.addProperty(res, 'id', 'GridGain External Repository');
    $generatorPom.addProperty(res, 'url', 'http://www.gridgainsystems.com/nexus/content/repositories/gridgain_staging-1555');
    res.endBlock('</repository>');
    res.endBlock('</repositories>');

    res.needEmptyLine = true;

    addDependency('org.apache.ignite', 'ignite-core', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-spring', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-indexing', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-rest-http', igniteVersion);

    if (_.find(cluster.igfss, function (igfs) { return igfs.secondaryFileSystemEnabled; }))
        addDependency('org.apache.ignite', 'ignite-hadoop', igniteVersion);

    if (_.find(caches, {"cacheStoreFactory" : {"kind" : "CacheHibernateBlobStoreFactory"}}))
        addDependency('org.apache.ignite', 'ignite-hibernate', igniteVersion);

    if (dialect.Generic)
        addDependency('com.mchange', 'c3p0', '0.9.5.1');

    if (dialect.MySQL)
        addDependency('mysql', 'mysql-connector-java', '5.1.37');

    if (dialect.PosgreSQL)
        addDependency('org.postgresql', 'postgresql', '9.4-1204-jdbc42');

    if (dialect.H2)
        addDependency('com.h2database', 'h2', '1.3.175');

    if (dialect.Oracle)
        addDependency('oracle', 'jdbc', '11.2', 'ojdbc6.jar');

    if (dialect.DB2)
        addDependency('ibm', 'jdbc', '4.19.26', 'db2jcc4.jar');

    if (dialect.SQLServer)
        addDependency('microsoft', 'jdbc', '4.1', 'sqljdbc41.jar');

    $generatorPom.dependencies(res, cluster, dependencies);

    res.needEmptyLine = true;

    res.startBlock('<build>');
    res.startBlock('<resources>');
    addResource('src/main/java', '**/*.java');
    addResource('src/main/resources');
    res.endBlock('</resources>');

    res.startBlock('<plugins>');
    res.startBlock('<plugin>');
    $generatorPom.addProperty(res, 'artifactId', 'maven-compiler-plugin');
    $generatorPom.addProperty(res, 'version', '3.1');
    res.startBlock('<configuration>');
    $generatorPom.addProperty(res, 'source', '1.7');
    $generatorPom.addProperty(res, 'target', '1.7');
    res.endBlock('</configuration>');
    res.endBlock('</plugin>');
    res.endBlock('</plugins>');
    res.endBlock('</build>');

    res.endBlock('</project>');

    return res;
};
