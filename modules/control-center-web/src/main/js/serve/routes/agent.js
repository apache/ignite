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

'use strict';

// Fire me up!

module.exports = {
    implements: 'agent-routes',
    inject: ['require(lodash)', 'require(express)', 'require(apache-ignite)', 'require(fs)', 'require(jszip)', 'settings', 'agent']
};

/**
 * @param _
 * @param express
 * @param apacheIgnite
 * @param fs
 * @param JSZip
 * @param settings
 * @param {AgentManager} agentMgr
 * @returns {Promise}
 */
module.exports.factory = function(_, express, apacheIgnite, fs, JSZip, settings, agentMgr) {
    return new Promise((resolveFactory) => {
        const router = new express.Router();

        const SqlFieldsQuery = apacheIgnite.SqlFieldsQuery;
        const ScanQuery = apacheIgnite.ScanQuery;

        const _client = (userId) => {
            return new Promise((resolve, reject) => {
                const agent = agentMgr.findClient(userId);

                if (agent)
                    return resolve(agent);

                reject({code: 503, message: 'Connection to Ignite Web Agent is not established'});
            });
        };

        const _compact = (className) => {
            return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
        };

        const _handleException = (res) => {
            return (error) => {
                if (_.isObject(error))
                    return res.status(error.code).send(error.message);

                return res.status(500).send(error);
            };
        };

        /* Get grid topology. */
        router.get('/download/zip', (req, res) => {
            const agentFld = settings.agent.file;
            const agentZip = agentFld + '.zip';
            const agentPathZip = 'public/agent/' + agentFld + '.zip';

            fs.stat(agentPathZip, (err, stats) => {
                if (err)
                    return res.download(agentPathZip, agentZip);

                // Read a zip file.
                fs.readFile(agentPathZip, (errFs, data) => {
                    if (errFs)
                        return res.download(agentPathZip, agentZip);

                    const zip = new JSZip(data);

                    const prop = [];

                    const host = req.hostname.match(/:/g) ? req.hostname.slice(0, req.hostname.indexOf(':')) : req.hostname;

                    prop.push('token=' + req.user.token);
                    prop.push('server-uri=wss://' + host + ':' + settings.agent.port);
                    prop.push('#Uncomment following options if needed:');
                    prop.push('#node-uri=http://localhost:8080');
                    prop.push('#driver-folder=./jdbc-drivers');
                    prop.push('');
                    prop.push('#Note: Do not change this auto generated line');
                    prop.push('rel-date=' + stats.birthtime.getTime());

                    zip.file(agentFld + '/default.properties', prop.join('\n'));

                    const buffer = zip.generate({type: 'nodebuffer', platform: 'UNIX'});

                    // Set the archive name.
                    res.attachment(agentZip);

                    res.send(buffer);
                });
            });
        });

        /* Get grid topology. */
        router.post('/topology', (req, res) => {
            _client(req.currentUserId())
                .then((client) => client.ignite(req.body.demo).cluster(req.body.attr, req.body.mtr))
                .then((clusters) => res.json(clusters))
                .catch(_handleException(res));
        });

        /* Execute query. */
        router.post('/query', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    // Create sql query.
                    const qry = new SqlFieldsQuery(req.body.query);

                    // Set page size for query.
                    qry.setPageSize(req.body.pageSize);

                    return client.ignite(req.body.demo).cache(req.body.cacheName).query(qry).nextPage();
                })
                .then((cursor) => res.json({
                    meta: cursor.fieldsMetadata(),
                    rows: cursor.page(),
                    queryId: cursor.queryId()
                }))
                .catch(_handleException(res));
        });

        /* Execute query getAll. */
        router.post('/query/getAll', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    // Create sql query.
                    const qry = req.body.query ? new SqlFieldsQuery(req.body.query) : new ScanQuery();

                    // Set page size for query.
                    qry.setPageSize(1024);

                    // Get query cursor.
                    const cursor = client.ignite(req.body.demo).cache(req.body.cacheName).query(qry);

                    return cursor.getAll()
                        .then((rows) => res.json({meta: cursor.fieldsMetadata(), rows}));
                })
                .catch(_handleException(res));
        });

        /* Execute query. */
        router.post('/scan', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    // Create sql query.
                    const qry = new ScanQuery();

                    // Set page size for query.
                    qry.setPageSize(req.body.pageSize);

                    // Get query cursor.
                    return client.ignite(req.body.demo).cache(req.body.cacheName).query(qry).nextPage();
                })
                .then((cursor) => res.json({
                    meta: cursor.fieldsMetadata(),
                    rows: cursor.page(),
                    queryId: cursor.queryId()
                }))
                .catch(_handleException(res));
        });

        /* Get next query page. */
        router.post('/query/fetch', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    const cache = client.ignite(req.body.demo).cache(req.body.cacheName);

                    const cmd = cache._createCommand('qryfetch')
                        .addParam('qryId', req.body.queryId)
                        .addParam('pageSize', req.body.pageSize);

                    return cache.__createPromise(cmd);
                })
                .then((page) => res.json({rows: page.items, last: page === null || page.last}))
                .catch(_handleException(res));
        });

        /* Close query cursor by id. */
        router.post('/query/close', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    const cache = client.ignite(req.body.demo).cache(req.body.cacheName);

                    return cache.__createPromise(cache._createCommand('qrycls').addParam('qryId', req.body.queryId));
                })
                .then(() => res.sendStatus(200))
                .catch(_handleException(res));
        });

        /* Get metadata for cache. */
        router.post('/cache/metadata', (req, res) => {
            _client(req.currentUserId())
                .then((client) => client.ignite(req.body.demo).cache(req.body.cacheName).metadata())
                .then((caches) => {
                    let types = [];

                    const _typeMapper = (meta, typeName) => {
                        let fields = meta.fields[typeName];

                        let columns = [];

                        for (const fieldName in fields) {
                            if (fields.hasOwnProperty(fieldName)) {
                                const fieldClass = _compact(fields[fieldName]);

                                columns.push({
                                    type: 'field',
                                    name: fieldName,
                                    clazz: fieldClass,
                                    system: fieldName === '_KEY' || fieldName === '_VAL',
                                    cacheName: meta.cacheName,
                                    typeName
                                });
                            }
                        }

                        const indexes = [];

                        for (const index of meta.indexes[typeName]) {
                            fields = [];

                            for (const field of index.fields) {
                                fields.push({
                                    type: 'index-field',
                                    name: field,
                                    order: index.descendings.indexOf(field) < 0,
                                    unique: index.unique,
                                    cacheName: meta.cacheName,
                                    typeName
                                });
                            }

                            if (fields.length > 0) {
                                indexes.push({
                                    type: 'index',
                                    name: index.name,
                                    children: fields,
                                    cacheName: meta.cacheName,
                                    typeName
                                });
                            }
                        }

                        columns = _.sortBy(columns, 'name');

                        if (!_.isEmpty(indexes)) {
                            columns = columns.concat({
                                type: 'indexes',
                                name: 'Indexes',
                                cacheName: meta.cacheName,
                                typeName,
                                children: indexes
                            });
                        }

                        return {
                            type: 'type',
                            cacheName: meta.cacheName || '',
                            typeName,
                            children: columns
                        };
                    };

                    for (const meta of caches) {
                        const cacheTypes = meta.types.map(_typeMapper.bind(null, meta));

                        if (!_.isEmpty(cacheTypes))
                            types = types.concat(cacheTypes);
                    }

                    res.json(types);
                })
                .catch(_handleException(res));
        });

        /* Ping client. */
        router.post('/ping', (req, res) => {
            _client(req.currentUserId())
                .then(() => res.sendStatus(200))
                .catch(_handleException(res));
        });

        /* Get JDBC drivers list. */
        router.post('/drivers', (req, res) => {
            _client(req.currentUserId())
                .then((client) => client.availableDrivers())
                .then((arr) => res.json(arr))
                .catch(_handleException(res));
        });

        /** Get database schemas. */
        router.post('/schemas', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    const args = req.body;

                    args.jdbcInfo = {user: args.user, password: args.password};

                    return client.metadataSchemas(args.jdbcDriverJar, args.jdbcDriverClass, args.jdbcUrl, args.jdbcInfo);
                })
                .then((arr) => res.json(arr))
                .catch(_handleException(res));
        });

        /** Get database tables. */
        router.post('/tables', (req, res) => {
            _client(req.currentUserId())
                .then((client) => {
                    const args = req.body;

                    args.jdbcInfo = {user: args.user, password: args.password};

                    return client.metadataTables(args.jdbcDriverJar, args.jdbcDriverClass, args.jdbcUrl, args.jdbcInfo, args.schemas, args.tablesOnly);
                })
                .then((arr) => res.json(arr))
                .catch(_handleException(res));
        });

        resolveFactory(router);
    });
};

