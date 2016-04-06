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
    implements: 'demo-routes',
    inject: ['require(lodash)', 'require(express)', 'settings', 'mongo']
};

module.exports.factory = (_, express, settings, mongo) => {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        const caches = [{
            cacheMode: 'LOCAL',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            name: 'local',
            sqlFunctionClasses: [],
            cacheStoreFactory: {
                CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}
            },
            domains: [],
            clusters: []
        }, {
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            name: 'partitioned',
            sqlFunctionClasses: [],
            cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}},
            domains: [],
            clusters: []
        }, {
            cacheMode: 'REPLICATED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            name: 'replicated',
            sqlFunctionClasses: [],
            cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}},
            domains: [],
            clusters: []
        }];

        const igfss = [{
            ipcEndpointEnabled: true,
            fragmentizerEnabled: true,
            name: 'igfs',
            dataCacheName: 'igfs-data',
            metaCacheName: 'igfs-meta',
            pathModes: [],
            clusters: []
        }];

        const clusters = [{
            name: 'cluster-igfs',
            connector: {noDelay: true},
            communication: {tcpNoDelay: true},
            igfss: [],
            caches: [],
            binaryConfiguration: {compactFooter: true, typeConfigurations: []},
            discovery: {
                kind: 'Multicast',
                Multicast: {addresses: []},
                Vm: {addresses: ['127.0.0.1:47500..47510']}
            }
        }, {
            name: 'cluster-caches',
            connector: {noDelay: true},
            communication: {tcpNoDelay: true},
            igfss: [],
            caches: [],
            binaryConfiguration: {compactFooter: true, typeConfigurations: []},
            discovery: {
                kind: 'Multicast',
                Multicast: {addresses: []},
                Vm: {addresses: ['127.0.0.1:47500..47510']}
            }
        }];

        /**
         * Reset demo configuration.
         */
        router.post('/reset', (req, res) => {
            mongo.spaces(req.user._id, true)
                .then((spaces) => {
                    if (spaces.length) {
                        const spaceIds = spaces.map((space) => space._id);

                        return Promise.all([
                            mongo.Cluster.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Cache.remove({space: {$in: spaceIds}}).exec(),
                            mongo.DomainModel.remove({space: {$in: spaceIds}}).exec(),
                            mongo.Igfs.remove({space: {$in: spaceIds}}).exec()
                        ]).then(() => spaces[0]);
                    }

                    return new mongo.Space({name: 'Demo space', owner: req.user._id, demo: true}).save();
                })
                .then((space) => {
                    return Promise.all(_.map(clusters, (cluster) => {
                        const clusterDoc = new mongo.Cluster(cluster);

                        clusterDoc.space = space._id;

                        return clusterDoc.save();
                    }));
                })
                .then((clusters) => {
                    return _.map(clusters, (cluster) => {
                        if (cluster.name.endsWith('-caches')) {
                            return Promise.all(_.map(caches, (cache) => {
                                    const cacheDoc = new mongo.Cache(cache);

                                    cacheDoc.space = cluster.space;
                                    cacheDoc.clusters.push(cluster._id);

                                    return cacheDoc.save();
                                }))
                                .then((cacheDocs) => {
                                    _.forEach(cacheDocs, (cacheDoc) => {cluster.caches.push(cacheDoc._id)});

                                    return cluster.save();
                                })
                        }

                        if (cluster.name.endsWith('-igfs')) {
                            return Promise.all(_.map(igfss, (igfs) => {
                                    const igfsDoc = new mongo.Igfs(igfs);

                                    igfsDoc.space = cluster.space;
                                    igfsDoc.clusters.push(cluster._id);

                                    return igfsDoc.save();
                                }))
                                .then((igfsDocs) => {
                                    _.forEach(igfsDocs, (igfsDocs) => {cluster.igfss.push(igfsDocs._id)});

                                    return cluster.save();
                                })
                        }
                    });
                })
                .then(() => res.sendStatus(200))
                .catch((err) => res.status(500).send(err.message));
        });

        factoryResolve(router);
    });
};

