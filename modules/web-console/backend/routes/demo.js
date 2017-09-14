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

const clusters = require('./demo/clusters.json');
const caches = require('./demo/caches.json');
const domains = require('./demo/domains.json');
const igfss = require('./demo/igfss.json');

module.exports = {
    implements: 'routes/demo',
    inject: ['require(lodash)', 'require(express)', 'settings', 'mongo', 'services/spaces', 'errors']
};

module.exports.factory = (_, express, settings, mongo, spacesService, errors) => {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Reset demo configuration.
         */
        router.post('/reset', (req, res) => {
            spacesService.spaces(req.user._id, true)
                .then((spaces) => {
                    const spaceIds = _.map(spaces, '_id');

                    return spacesService.cleanUp(spaceIds)
                        .then(() => mongo.Space.remove({_id: {$in: _.tail(spaceIds)}}).exec())
                        .then(() => _.head(spaces));
                })
                .catch((err) => {
                    if (err instanceof errors.MissingResourceException)
                        return spacesService.createDemoSpace(req.user._id);

                    throw err;
                })
                .then((space) => {
                    return Promise.all(_.map(clusters, (cluster) => {
                        const clusterDoc = new mongo.Cluster(cluster);

                        clusterDoc.space = space._id;

                        return clusterDoc.save();
                    }));
                })
                .then((clusterDocs) => {
                    return _.map(clusterDocs, (cluster) => {
                        const addCacheToCluster = (cacheDoc) => cluster.caches.push(cacheDoc._id);
                        const addIgfsToCluster = (igfsDoc) => cluster.igfss.push(igfsDoc._id);

                        if (cluster.name.endsWith('-caches')) {
                            const cachePromises = _.map(caches, (cacheData) => {
                                const cache = new mongo.Cache(cacheData);

                                cache.space = cluster.space;
                                cache.clusters.push(cluster._id);

                                return cache.save()
                                    .then((cacheDoc) => {
                                        const domainData = _.find(domains, (item) =>
                                            item.databaseTable === cacheDoc.name.slice(0, -5).toUpperCase());

                                        if (domainData) {
                                            const domain = new mongo.DomainModel(domainData);

                                            domain.space = cacheDoc.space;
                                            domain.caches.push(cacheDoc._id);

                                            return domain.save()
                                                .then((domainDoc) => {
                                                    cacheDoc.domains.push(domainDoc._id);

                                                    return cacheDoc.save();
                                                });
                                        }

                                        return cacheDoc;
                                    });
                            });

                            return Promise.all(cachePromises)
                                .then((cacheDocs) => {
                                    _.forEach(cacheDocs, addCacheToCluster);

                                    return cluster.save();
                                });
                        }

                        if (cluster.name.endsWith('-igfs')) {
                            return Promise.all(_.map(igfss, (igfs) => {
                                const igfsDoc = new mongo.Igfs(igfs);

                                igfsDoc.space = cluster.space;
                                igfsDoc.clusters.push(cluster._id);

                                return igfsDoc.save();
                            }))
                            .then((igfsDocs) => {
                                _.forEach(igfsDocs, addIgfsToCluster);

                                return cluster.save();
                            });
                        }
                    });
                })
                .then(() => res.sendStatus(200))
                .catch((err) => res.status(500).send(err.message));
        });

        factoryResolve(router);
    });
};

