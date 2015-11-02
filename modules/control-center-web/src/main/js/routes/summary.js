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

var db = require('../db');

var router = require('express').Router();

var $generatorXml = require('./generator/generator-xml');
var $generatorJava = require('./generator/generator-java');
var $generatorDocker = require('./generator/generator-docker');
var $generatorProperties = require('./generator/generator-properties');
var $generatorPom = require('./generator/generator-pom');
var $generatorReadme = require('./generator/generator-readme');

// GET template for summary tabs.
router.get('/summary-tabs', function (req, res) {
    res.render('configuration/summary-tabs', {});
});

/* GET summary page. */
router.get('/', function (req, res) {
    res.render('configuration/summary');
});

router.post('/download', function (req, res) {
    // Get cluster with all inner objects (caches, metadata).
    db.Cluster.findById(req.body._id).deepPopulate('caches caches.metadatas').exec(function (err, cluster) {
        if (err)
            return res.status(500).send(err.message);

        if (!cluster)
            return res.sendStatus(404);

        var clientNearConfiguration = JSON.parse(req.body.clientNearConfiguration);

        var JSZip = require('jszip');

        var zip = new JSZip();

        // Set the archive name.
        res.attachment(cluster.name + '-configuration.zip');

        var builder = $generatorProperties.sslProperties(cluster);

        zip.file('Dockerfile', $generatorDocker.clusterDocker(cluster, req.body.os));

        builder = $generatorProperties.dataSourcesProperties(cluster, builder);

        if (builder)
            zip.file('src/main/resources/secret.properties', builder.asString());

        var srcPath = 'src/main/java/';

        zip.file('config/' + cluster.name + '-server.xml', $generatorXml.cluster(cluster));
        zip.file('config/' + cluster.name + '-client.xml', $generatorXml.cluster(cluster, clientNearConfiguration));
        zip.file(srcPath + 'ConfigurationFactory.java', $generatorJava.cluster(cluster, 'ServerConfigurationFactory'));
        zip.file(srcPath + 'ClientConfigurationFactory.java', $generatorJava.cluster(cluster, 'ClientConfigurationFactory', clientNearConfiguration));
        zip.file('pom.xml', $generatorPom.pom(cluster.caches, '1.5.0').asString());

        zip.file('README.txt', $generatorReadme.readme().asString());
        zip.file('jdbc-drivers/README.txt', 'Copy proprietary JDBC drivers to this folder.');

        $generatorJava.pojos(cluster.caches, req.body.useConstructor, req.body.includeKeyFields);

        var metadatas = $generatorJava.metadatas;

        for (var metaIx = 0; metaIx < metadatas.length; metaIx ++) {
            var meta = metadatas[metaIx];

            if (meta.keyClass)
                zip.file(srcPath + meta.keyType.replace(/\./g, '/') + '.java', meta.keyClass);

            zip.file(srcPath + meta.valueType.replace(/\./g, '/') + '.java', meta.valueClass);
        }

        var buffer = zip.generate({type:"nodebuffer"});

        res.send(buffer);
    });
});

module.exports = router;
