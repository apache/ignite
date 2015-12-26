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

var _ = require('lodash');
var router = require('express').Router();
var agentManager = require('../agents/agent-manager');

var apacheIgnite = require('apache-ignite');
var SqlFieldsQuery = apacheIgnite.SqlFieldsQuery;
var ScanQuery = apacheIgnite.ScanQuery;

function _client(req, res) {
    var client = agentManager.getAgentManager().findClient(req.currentUserId());

    if (!client) {
        res.status(503).send('Connection to Ignite Web Agent is not established');

        return null;
    }

    return client;
}

function _compact(className) {
    return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
}

/* Get grid topology. */
router.get('/download/zip', function (req, res) {
    var fs = require('fs');
    var JSZip = require('jszip');
    var config = require('../helpers/configuration-loader.js');

    var agentFld = 'ignite-web-agent-1.5.0.final-SNAPSHOT';
    var agentZip = agentFld + '.zip';

    // Read a zip file.
    fs.readFile('public/agent/' + agentZip, function(err, data) {
        if (err)
            return res.download('public/agent/' + agentZip, agentZip);

        var zip = new JSZip(data);

        var prop = [];

        var host = req.hostname.match(/:/g) ? req.hostname.slice(0, req.hostname.indexOf(':')) : req.hostname;

        prop.push('token=' + req.user.token);
        prop.push('server-uri=wss://' + host + ':' + config.get('agent-server:port'));
        prop.push('#Uncomment following options if needed:');
        prop.push('#node-uri=http://localhost:8080');
        prop.push('#driver-folder=./jdbc-drivers');

        zip.file(agentFld + '/default.properties', prop.join('\n'));

        var buffer = zip.generate({type:"nodebuffer", platform: "UNIX"});

        // Set the archive name.
        res.attachment(agentZip);

        res.send(buffer);
    });
});

/* Get grid topology. */
router.post('/topology', function (req, res) {
    var client = _client(req, res);

    if (client) {
        client.ignite().cluster(req.body.attr, req.body.mtr).then(
            function (clusters) {
                res.json(clusters);
            },
            function (err) {
            var mStatusCode = /.*Status code:\s+(\d+)(?:\s|$)/g.exec(err);

            res.status(mStatusCode != null && mStatusCode[1] ? mStatusCode[1] : 500).send(err);
        });
    }
});

/* Execute query. */
router.post('/query', function (req, res) {
    var client = _client(req, res);

    if (client) {
        // Create sql query.
        var qry = new SqlFieldsQuery(req.body.query);

        // Set page size for query.
        qry.setPageSize(req.body.pageSize);

        // Get query cursor.
        client.ignite().cache(req.body.cacheName).query(qry).nextPage().then(function (cursor) {
            res.json({meta: cursor.fieldsMetadata(), rows: cursor.page(), queryId: cursor.queryId()});
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Execute query getAll. */
router.post('/query/getAll', function (req, res) {
    var client = _client(req, res);

    if (client) {
        // Create sql query.
        var qry = new SqlFieldsQuery(req.body.query);

        // Set page size for query.
        qry.setPageSize(1024);

        // Get query cursor.
        var cursor = client.ignite().cache(req.body.cacheName).query(qry);

        cursor.getAll().then(function (rows) {
            res.json({meta: cursor.fieldsMetadata(), rows: rows});
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Execute query. */
router.post('/scan', function (req, res) {
    var client = _client(req, res);

    if (client) {
        // Create sql query.
        var qry = new ScanQuery();

        // Set page size for query.
        qry.setPageSize(req.body.pageSize);

        // Get query cursor.
        client.ignite().cache(req.body.cacheName).query(qry).nextPage().then(function (cursor) {
            res.json({meta: cursor.fieldsMetadata(), rows: cursor.page(), queryId: cursor.queryId()});
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Get next query page. */
router.post('/query/fetch', function (req, res) {
    var client = _client(req, res);

    if (client) {
        var cache = client.ignite().cache(req.body.cacheName);

        var cmd = cache._createCommand('qryfetch').addParam('qryId', req.body.queryId).
            addParam('pageSize', req.body.pageSize);

        cache.__createPromise(cmd).then(function (page) {
            res.json({rows: page['items'], last: page === null || page['last']});
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Close query cursor by id. */
router.post('/query/close', function (req, res) {
    var client = _client(req, res);

    if (client) {
        var cache = client.ignite().cache(req.body.cacheName);

        var cmd = cache._createCommand('qrycls').addParam('qryId', req.body.queryId);

        cache.__createPromise(cmd).then(function () {
            res.sendStatus(200);
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Get metadata for cache. */
router.post('/cache/metadata', function (req, res) {
    var client = _client(req, res);

    if (client) {
        client.ignite().cache(req.body.cacheName).metadata().then(function (caches) {
            var types = [];

            for (var meta of caches) {
                var cacheTypes = meta.types.map(function (typeName) {
                    var fields = meta.fields[typeName];

                    var columns = [];

                    for (var fieldName in fields) {
                        var fieldClass = _compact(fields[fieldName]);

                        columns.push({
                            type: 'field',
                            name: fieldName,
                            clazz: fieldClass,
                            system: fieldName == "_KEY" || fieldName == "_VAL",
                            cacheName: meta.cacheName,
                            typeName: typeName
                        });
                    }

                    var indexes = [];

                    for (var index of meta.indexes[typeName]) {
                        fields = [];

                        for (var field of index.fields) {
                            fields.push({
                                type: 'index-field',
                                name: field,
                                order: index.descendings.indexOf(field) < 0,
                                unique: index.unique,
                                cacheName: meta.cacheName,
                                typeName: typeName
                            });
                        }

                        if (fields.length > 0)
                            indexes.push({
                                type: 'index',
                                name: index.name,
                                children: fields,
                                cacheName: meta.cacheName,
                                typeName: typeName
                            });
                    }

                    columns = _.sortBy(columns, 'name');

                    if (!_.isEmpty(indexes))
                        columns = columns.concat({type: 'indexes', name: 'Indexes', cacheName: meta.cacheName, typeName: typeName, children: indexes });

                    return {type: 'type', cacheName: meta.cacheName,  typeName: typeName, children: columns };
                });

                if (!_.isEmpty(cacheTypes))
                    types = types.concat(cacheTypes);
            }

            res.json(types);
        }, function (err) {
            res.status(500).send(err);
        });
    }
});

/* Ping client. */
router.post('/ping', function (req, res) {
    if (_client(req, res) != null)
        res.sendStatus(200);
});

/* Enable test-drive for sql. */
router.post('/testdrive/sql', function (req, res) {
    var client = _client(req, res);

    if (client) {
        client.enableTestDriveSQL(function (err, enabled) {
            if (err)
                return res.status(500).send(err);

            res.status(200).send(enabled);
        });
    }
});

/* Get JDBC drivers list. */
router.post('/drivers', function (req, res) {
    var client = _client(req, res);

    if (client) {
        client.availableDrivers(function (err, drivers) {
            if (err)
                return res.status(500).send(err);

            res.json(drivers);
        });
    }
});

/** Get database schemas. */
router.post('/schemas', function (req, res) {
    var client = _client(req, res);

    if (client) {
        var params = req.body;

        client.metadataSchemas(params.jdbcDriverJar, params.jdbcDriverClass, params.jdbcUrl, {user: params.user, password: params.password}, function (err, meta) {
            if (err)
                return res.status(500).send(err);

            res.json(meta);
        });
    }
});

/** Get database metadata. */
router.post('/metadata', function (req, res) {
    var client = _client(req, res);

    if (client) {
        var params = req.body;

        client.metadataTables(params.jdbcDriverJar, params.jdbcDriverClass, params.jdbcUrl,
            {user: params.user, password: params.password}, params.schemas, params.tablesOnly,
            function (err, meta) {
                if (err)
                    return res.status(500).send(err);

                res.json(meta);
            });
    }
});

module.exports = router;
