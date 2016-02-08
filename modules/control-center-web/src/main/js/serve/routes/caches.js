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

// Fire me up!

module.exports = {
    implements: 'caches-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function (_, express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();
        
        /**
         * Get spaces and caches accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
                if (mongo.processed(err, res)) {
                    var space_ids = spaces.map(function (value) {
                        return value._id;
                    });

                    // Get all clusters for spaces.
                    mongo.Cluster.find({space: {$in: space_ids}}, '_id name caches').sort('name').exec(function (err, clusters) {
                        if (mongo.processed(err, res)) {
                            // Get all domain models for spaces.
                            mongo.DomainModel.find({space: {$in: space_ids}}).sort('name').exec(function (err, domains) {
                                if (mongo.processed(err, res)) {
                                    // Get all caches for spaces.
                                    mongo.Cache.find({space: {$in: space_ids}}).sort('name').exec(function (err, caches) {
                                        if (mongo.processed(err, res)) {
                                            _.forEach(clusters, function (cluster) {
                                                cluster.caches = _.filter(cluster.caches, function (cacheId) {
                                                    return _.find(caches, {_id: cacheId});
                                                });
                                            });

                                            _.forEach(domains, function (domain) {
                                                domain.caches = _.filter(domain.caches, function (cacheId) {
                                                    return _.find(caches, {_id: cacheId});
                                                });
                                            });

                                            _.forEach(caches, function (cache) {
                                                // Remove deleted clusters.
                                                cache.clusters = _.filter(cache.clusters, function (clusterId) {
                                                    return _.findIndex(clusters, function (cluster) {
                                                            return cluster._id.equals(clusterId);
                                                        }) >= 0;
                                                });

                                                // Remove deleted domain models.
                                                cache.domains = _.filter(cache.domains, function (metaId) {
                                                    return _.findIndex(domains, function (domain) {
                                                            return domain._id.equals(metaId);
                                                        }) >= 0;
                                                });
                                            });

                                            res.json({
                                                spaces: spaces,
                                                clusters: clusters.map(function (cluster) {
                                                    return {
                                                        value: cluster._id,
                                                        label: cluster.name,
                                                        caches: cluster.caches
                                                    };
                                                }),
                                                domains: domains,
                                                caches: caches
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
        router.post('/save', function (req, res) {
            var params = req.body;
            var cacheId = params._id;
            var clusters = params.clusters;
            var domains = params.domains;

            if (params._id) {
                mongo.Cache.update({_id: cacheId}, params, {upsert: true}, function (err) {
                    if (mongo.processed(err, res))
                        mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                            if (mongo.processed(err, res))
                                mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {caches: cacheId}}, {multi: true}, function (err) {
                                    if (mongo.processed(err, res))
                                        mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                            if (mongo.processed(err, res))
                                                mongo.DomainModel.update({_id: {$nin: domains}}, {$pull: {caches: cacheId}}, {multi: true}, function (err) {
                                                    if (mongo.processed(err, res))
                                                        res.send(params._id);
                                                });
                                        });
                                });
                        });
                })
            }
            else
                mongo.Cache.findOne({space: params.space, name: params.name}, function (err, cache) {
                    if (mongo.processed(err, res)) {
                        if (cache)
                            return res.status(500).send('Cache with name: "' + cache.name + '" already exist.');

                        (new mongo.Cache(params)).save(function (err, cache) {
                            if (mongo.processed(err, res)) {
                                cacheId = cache._id;

                                mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                    if (mongo.processed(err, res))
                                        mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                            if (mongo.processed(err, res))
                                                res.send(cacheId);
                                        });
                                });
                            }
                        });
                    }
                });
        });

        /**
         * Remove cache by ._id.
         */
        router.post('/remove', function (req, res) {
            mongo.Cache.remove(req.body, function (err) {
                if (mongo.processed(err, res))
                    res.sendStatus(200);
            })
        });

        /**
         * Remove all caches.
         */
        router.post('/remove/all', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
                if (mongo.processed(err, res)) {
                    var space_ids = spaces.map(function (value) {
                        return value._id;
                    });

                    mongo.Cache.remove({space: {$in: space_ids}}, function (err) {
                        if (err)
                            return res.status(500).send(err.message);

                        mongo.Cluster.update({space: {$in: space_ids}}, {caches: []}, {multi: true}, function (err) {
                            if (mongo.processed(err, res))
                                mongo.DomainModel.update({space: {$in: space_ids}}, {caches: []}, {multi: true}, function (err) {
                                    if (mongo.processed(err, res))
                                        res.sendStatus(200);
                                });
                        });
                    })
                }
            });
        });

        resolve(router);
    });
};

