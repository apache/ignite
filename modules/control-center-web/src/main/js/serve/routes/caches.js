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
    implements: 'caches-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function(_, express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get spaces and caches accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    // Get all clusters for spaces.
                    mongo.Cluster.find({space: {$in: space_ids}}, '_id name caches').sort('name').exec((errCluster, clusters) => {
                        if (mongo.processed(errCluster, res)) {
                            // Get all domain models for spaces.
                            mongo.DomainModel.find({space: {$in: space_ids}}).sort('name').exec((errDomainModel, domains) => {
                                if (mongo.processed(errDomainModel, res)) {
                                    // Get all caches for spaces.
                                    mongo.Cache.find({space: {$in: space_ids}}).sort('name').exec((err, caches) => {
                                        if (mongo.processed(err, res)) {
                                            _.forEach(clusters, (cluster) => {
                                                cluster.caches = _.filter(cluster.caches, (cacheId) => {
                                                    return _.find(caches, {_id: cacheId});
                                                });
                                            });

                                            _.forEach(domains, (domain) => {
                                                domain.caches = _.filter(domain.caches, (cacheId) => {
                                                    return _.find(caches, {_id: cacheId});
                                                });
                                            });

                                            _.forEach(caches, (cache) => {
                                                // Remove deleted clusters.
                                                cache.clusters = _.filter(cache.clusters, (clusterId) => {
                                                    return _.findIndex(clusters, (cluster) => {
                                                        return cluster._id.equals(clusterId);
                                                    }) >= 0;
                                                });

                                                // Remove deleted domain models.
                                                cache.domains = _.filter(cache.domains, (metaId) => {
                                                    return _.findIndex(domains, (domain) => {
                                                        return domain._id.equals(metaId);
                                                    }) >= 0;
                                                });
                                            });

                                            res.json({
                                                spaces,
                                                clusters: clusters.map((cluster) => {
                                                    return {
                                                        value: cluster._id,
                                                        label: cluster.name,
                                                        caches: cluster.caches
                                                    };
                                                }),
                                                domains,
                                                caches
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        });

        /**
         * Save cache.
         */
        router.post('/save', (req, res) => {
            const params = req.body;
            const clusters = params.clusters;
            const domains = params.domains;
            let cacheId = params._id;

            if (params._id) {
                mongo.Cache.update({_id: cacheId}, params, {upsert: true}, (errCache) => {
                    if (mongo.processed(errCache, res)) {
                        mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errClusterAdd) => {
                            if (mongo.processed(errClusterAdd, res)) {
                                mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {caches: cacheId}}, {multi: true}, (errClusterPull) => {
                                    if (mongo.processed(errClusterPull, res)) {
                                        mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errDomainModelAdd) => {
                                            if (mongo.processed(errDomainModelAdd, res)) {
                                                mongo.DomainModel.update({_id: {$nin: domains}}, {$pull: {caches: cacheId}}, {multi: true}, (errDomainModelPull) => {
                                                    if (mongo.processed(errDomainModelPull, res))
                                                        res.send(params._id);
                                                });
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
            else {
                mongo.Cache.findOne({space: params.space, name: params.name}, (errCacheFind, cacheFound) => {
                    if (mongo.processed(errCacheFind, res)) {
                        if (cacheFound)
                            return res.status(500).send('Cache with name: "' + cacheFound.name + '" already exist.');

                        (new mongo.Cache(params)).save((errCacheSave, cache) => {
                            if (mongo.processed(errCacheSave, res)) {
                                cacheId = cache._id;

                                mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errCluster) => {
                                    if (mongo.processed(errCluster, res)) {
                                        mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errDomainModel) => {
                                            if (mongo.processed(errDomainModel, res))
                                                res.send(cacheId);
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });

        /**
         * Remove cache by ._id.
         */
        router.post('/remove', (req, res) => {
            mongo.Cache.remove(req.body, (err) => {
                if (mongo.processed(err, res))
                    res.sendStatus(200);
            });
        });

        /**
         * Remove all caches.
         */
        router.post('/remove/all', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => {
                        return value._id;
                    });

                    mongo.Cache.remove({space: {$in: space_ids}}, (errCache) => {
                        if (errCache)
                            return res.status(500).send(errCache.message);

                        mongo.Cluster.update({space: {$in: space_ids}}, {caches: []}, {multi: true}, (errCluster) => {
                            if (mongo.processed(errCluster, res)) {
                                mongo.DomainModel.update({space: {$in: space_ids}}, {caches: []}, {multi: true}, (errDomainModel) => {
                                    if (mongo.processed(errDomainModel, res))
                                        res.sendStatus(200);
                                });
                            }
                        });
                    });
                }
            });
        });

        resolve(router);
    });
};

