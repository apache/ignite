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

/**
 * Module interaction with browsers.
 */
module.exports = {
    implements: 'browser-manager',
    inject: ['require(lodash)', 'require(socket.io)', 'agent-manager', 'configure']
};

module.exports.factory = (_, socketio, agentMgr, configure) => {
    const _errorToJson = (err) => {
        return {
            message: err.message || err,
            code: err.code || 1
        };
    };

    return {
        attach: (server) => {
            const io = socketio(server);

            configure.socketio(io);

            io.sockets.on('connection', (socket) => {
                const user = socket.client.request.user;

                const demo = socket.client.request._query.IgniteDemoMode === 'true';

                // Return available drivers to browser.
                socket.on('schemaImport:drivers', (cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.availableDrivers())
                        .then((drivers) => cb(null, drivers))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Return schemas from database to browser.
                socket.on('schemaImport:schemas', (preset, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const jdbcInfo = {user: preset.user, password: preset.password};

                            return agent.metadataSchemas(preset.jdbcDriverJar, preset.jdbcDriverClass, preset.jdbcUrl, jdbcInfo);
                        })
                        .then((schemas) => cb(null, schemas))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Return tables from database to browser.
                socket.on('schemaImport:tables', (preset, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const jdbcInfo = {user: preset.user, password: preset.password};

                            return agent.metadataTables(preset.jdbcDriverJar, preset.jdbcDriverClass, preset.jdbcUrl, jdbcInfo,
                                preset.schemas, preset.tablesOnly);
                        })
                        .then((tables) => cb(null, tables))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Return topology command result from grid to browser.
                socket.on('node:topology', (attr, mtr, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.topology(demo, attr, mtr))
                        .then((clusters) => cb(null, clusters))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Close query on node.
                socket.on('node:query:close', (queryId, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.queryClose(demo, queryId))
                        .then(() => cb())
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Execute query on node and return first page to browser.
                socket.on('node:query', (cacheName, pageSize, query, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            if (query === null)
                                return agent.scan(demo, cacheName, pageSize);

                            return agent.fieldsQuery(demo, cacheName, query, pageSize);
                        })
                        .then((res) => cb(null, res))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Fetch next page for query and return result to browser.
                socket.on('node:query:fetch', (queryId, pageSize, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.queryFetch(demo, queryId, pageSize))
                        .then((res) => cb(null, res))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Execute query on node and return full result to browser.
                socket.on('node:query:getAll', (cacheName, query, cb) => {
                    // Set page size for query.
                    const pageSize = 1024;

                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const firstPage = query === null ? agent.scan(demo, cacheName, pageSize)
                                : agent.fieldsQuery(demo, cacheName, query, pageSize);

                            const fetchResult = (acc) => {
                                if (acc.last)
                                    return acc;

                                return agent.queryFetch(demo, acc.queryId, pageSize)
                                    .then((res) => {
                                        acc.rows = acc.rows.concat(res.rows);

                                        acc.last = res.last;

                                        return fetchResult(acc);
                                    });
                            };

                            return firstPage
                                .then(fetchResult);
                        })
                        .then((res) => cb(null, res))
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Return cache metadata from all nodes in grid.
                socket.on('node:cache:metadata', (cacheName, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.metadata(demo, cacheName))
                        .then((caches) => {
                            let types = [];

                            const _compact = (className) => {
                                return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
                            };

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

                            return cb(null, types);
                        })
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Fetch next page for query and return result to browser.
                socket.on('node:visor:collect', (evtOrderKey, evtThrottleCntrKey, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.collect(demo, evtOrderKey, evtThrottleCntrKey))
                        .then((data) => {
                            if (data.finished)
                                return cb(null, data.result);

                            cb(_errorToJson(data.error));
                        })
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Clear specified cache on specified node and return result to browser.
                socket.on('node:cache:clear', (nid, cacheName, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.cacheClear(demo, nid, cacheName))
                        .then((data) => {
                            if (data.finished)
                                return cb(null, data.result);

                            cb(_errorToJson(data.error));
                        })
                        .catch((err) => cb(_errorToJson(err)));
                });

                // Stop specified cache on specified node and return result to browser.
                socket.on('node:cache:stop', (nids, cacheName, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.cacheStop(demo, nids, cacheName))
                        .then((data) => {
                            if (data.finished)
                                return cb(null, data.result);

                            cb(_errorToJson(data.error));
                        })
                        .catch((err) => cb(_errorToJson(err)));
                });


                // Ping node and return result to browser.
                socket.on('node:ping', (nid, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.ping(demo, nid))
                        .then((data) => {
                            if (data.finished)
                                return cb(null, data.result);

                            cb(_errorToJson(data.error));
                        })
                        .catch((err) => cb(_errorToJson(err)));
                });

                const count = agentMgr.addAgentListener(user._id, socket);

                socket.emit('agent:count', {count});
            });

            // Handle browser disconnect event.
            io.sockets.on('disconnect', (socket) =>
                agentMgr.removeAgentListener(socket.client.request.user._id, socket)
            );
        }
    };
};
