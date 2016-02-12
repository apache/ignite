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
    implements: 'clusters-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function(_, express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get spaces and clusters accessed for user account.
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

                    // Get all caches for spaces.
                    mongo.Cache.find({space: {$in: space_ids}}).sort('name').deepPopulate('domains').exec((errCache, caches) => {
                        if (mongo.processed(errCache, res)) {
                            // Get all IGFSs for spaces.
                            mongo.Igfs.find({space: {$in: space_ids}}).sort('name').exec((errIgfs, igfss) => {
                                if (mongo.processed(errIgfs, res)) {
                                    // Get all clusters for spaces.
                                    mongo.Cluster.find({space: {$in: space_ids}}).sort('name').deepPopulate(mongo.ClusterDefaultPopulate).exec((errCluster, clusters) => {
                                        if (mongo.processed(errCluster, res)) {
                                            _.forEach(caches, (cache) => {
                                                // Remove deleted caches.
                                                cache.clusters = _.filter(cache.clusters, (clusterId) => {
                                                    return _.find(clusters, {_id: clusterId});
                                                });
                                            });

                                            _.forEach(igfss, (igfs) => {
                                                // Remove deleted caches.
                                                igfs.clusters = _.filter(igfs.clusters, (clusterId) => {
                                                    return _.find(clusters, {_id: clusterId});
                                                });
                                            });

                                            _.forEach(clusters, (cluster) => {
                                                // Remove deleted caches.
                                                cluster.caches = _.filter(cluster.caches, (cacheId) => {
                                                    return _.find(caches, {_id: cacheId});
                                                });

                                                // Remove deleted IGFS.
                                                cluster.igfss = _.filter(cluster.igfss, (igfsId) => {
                                                    return _.findIndex(igfss, (igfs) => {
                                                        return igfs._id.equals(igfsId);
                                                    }) >= 0;
                                                });
                                            });

                                            res.json({ spaces, caches, igfss, clusters });
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
         * Save cluster.
         */
        router.post('/save', (req, res) => {
            const params = req.body;
            const caches = params.caches;
            let clusterId = params._id;

            if (params._id) {
                mongo.Cluster.update({_id: params._id}, params, {upsert: true}, (errCluster) => {
                    if (mongo.processed(errCluster, res)) {
                        mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, (errCacheAdd) => {
                            if (mongo.processed(errCacheAdd, res)) {
                                mongo.Cache.update({_id: {$nin: caches}}, {$pull: {clusters: clusterId}}, {multi: true}, (errCachePull) => {
                                    if (mongo.processed(errCachePull, res))
                                        res.send(params._id);
                                });
                            }
                        });
                    }
                });
            }
            else {
                mongo.Cluster.findOne({space: params.space, name: params.name}, (errClusterFound, clusterFound) => {
                    if (mongo.processed(errClusterFound, res)) {
                        if (clusterFound)
                            return res.status(500).send('Cluster with name: "' + clusterFound.name + '" already exist.');

                        (new mongo.Cluster(params)).save((errCluster, cluster) => {
                            if (mongo.processed(errCluster, res)) {
                                clusterId = cluster._id;

                                mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, (errCacheAdd) => {
                                    if (mongo.processed(errCacheAdd, res))
                                        res.send(clusterId);
                                });
                            }
                        });
                    }
                });
            }
        });

        /**
         * Remove cluster by ._id.
         */
        router.post('/remove', (req, res) => {
            mongo.Cluster.remove(req.body, (err) => {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            });
        });

        /**
         * Remove all clusters.
         */
        router.post('/remove/all', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    mongo.Cluster.remove({space: {$in: space_ids}}, (errCluster) => {
                        if (errCluster)
                            return res.status(500).send(errCluster.message);

                        mongo.Cache.update({space: {$in: space_ids}}, {clusters: []}, {multi: true}, (errCache) => {
                            if (mongo.processed(errCache, res)) {
                                mongo.Igfs.update({space: {$in: space_ids}}, {clusters: []}, {multi: true}, (errIgfs) => {
                                    if (mongo.processed(errIgfs, res))
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
