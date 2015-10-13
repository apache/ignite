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

        var clientNearConfiguration = req.body.clientNearConfiguration;

        var JSZip = require('jszip');

        var zip = new JSZip();

        // Set the archive name.
        res.attachment(cluster.name + (clientNearConfiguration ? '-client' : '-server') + '-configuration.zip');

        var builder = $generatorProperties.sslProperties(cluster);

        if (!clientNearConfiguration) {
            zip.file('Dockerfile', $generatorDocker.clusterDocker(cluster, req.body.os));

            builder = $generatorProperties.dataSourcesProperties(cluster, builder);
        }

        if (builder)
            zip.file('secret.properties', builder.asString());

        zip.file(cluster.name + '.xml', $generatorXml.cluster(cluster, clientNearConfiguration));
        zip.file(cluster.name + '.snippet.java', $generatorJava.cluster(cluster, false, clientNearConfiguration));
        zip.file('ConfigurationFactory.java', $generatorJava.cluster(cluster, true, clientNearConfiguration));

        $generatorJava.pojos(cluster.caches, req.body.useConstructor, req.body.includeKeyFields);

        var metadatas = $generatorJava.metadatas;

        for (var metaIx = 0; metaIx < metadatas.length; metaIx ++) {
            var meta = metadatas[metaIx];

            if (meta.keyClass)
                zip.file(meta.keyType.replace(/\./g, '/') + '.java', meta.keyClass);

            zip.file(meta.valueType.replace(/\./g, '/') + '.java', meta.valueClass);
        }

        var buffer = zip.generate({type:"nodebuffer"});

        res.send(buffer);
    });
});

module.exports = router;
