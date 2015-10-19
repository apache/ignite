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

// For server side we should load required libraries.
if (typeof window === 'undefined') {
    $generatorCommon = require('./generator-common');
}

// pom.xml generation entry point.
$generatorPom = {};

/**
 * Generate pom.xml.
 *
 * @param res Resulting output with generated pom.
 * @param igniteVersion Ignite version for Ignite dependencies.
 * @returns {string} Generated content.
 */
$generatorPom.pom = function (igniteVersion, res) {
    if (!res)
        res = $generatorCommon.builder();

    function addProperty(tag, val) {
        res.line('<' + tag + '>' + val + '</' + tag + '>');
    }

    function addDependency(groupId, artifactId, version) {
        res.startBlock('<dependency>');
        addProperty('groupId', groupId);
        addProperty('artifactId', artifactId);
        addProperty('version', version);
        res.endBlock('</dependency>');
    }

    function addResource(dir, exclude) {
        res.startBlock('<resource>');
        if (dir)
            addProperty('directory', dir);

        if (exclude) {
            res.startBlock('<excludes>');
            addProperty('exclude', exclude);
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

    addProperty('groupId', 'org.apache.ignite');
    addProperty('artifactId', 'ignite-generated-model');
    addProperty('version', igniteVersion);

    res.needEmptyLine = true;

    res.startBlock('<dependencies>');
    addDependency('org.apache.ignite', 'ignite-core', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-spring', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-indexing', igniteVersion);
    addDependency('org.apache.ignite', 'ignite-rest-http', igniteVersion);
    res.endBlock('</dependencies>');

    res.needEmptyLine = true;

    res.startBlock('<build>');
    res.startBlock('<resources>');
    addResource('src/main/java', '**/*.java');
    addResource('src/main/resources');
    res.endBlock('</resources>');

    res.startBlock('<plugins>');
    res.startBlock('<plugin>');
    addProperty('artifactId', 'maven-compiler-plugin');
    addProperty('version', '3.1');
    res.startBlock('<configuration>');
    addProperty('source', '1.7');
    addProperty('target', '1.7');
    res.endBlock('</configuration>');
    res.endBlock('</plugin>');
    res.endBlock('</plugins>');
    res.endBlock('</build>');

    res.endBlock('</project>');

    return res;
};

// For server side we should export properties generation entry point.
if (typeof window === 'undefined') {
    module.exports = $generatorPom;
}
