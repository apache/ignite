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
    implements: 'clusters-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function (_, express, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();
        
        /**
         * Get spaces and clusters accessed for user account.
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

                    // Get all caches for spaces.
                    mongo.Cache.find({space: {$in: space_ids}}).sort('name').deepPopulate('domains').exec(function (err, caches) {
                        if (mongo.processed(err, res)) {
                            // Get all IGFSs for spaces.
                            mongo.Igfs.find({space: {$in: space_ids}}).sort('name').exec(function (err, igfss) {
                                if (mongo.processed(err, res))
                                // Get all clusters for spaces.
                                    mongo.Cluster.find({space: {$in: space_ids}}).sort('name').deepPopulate(mongo.ClusterDefaultPopulate).exec(function (err, clusters) {
                                        if (mongo.processed(err, res)) {
                                            _.forEach(caches, function (cache) {
                                                // Remove deleted caches.
                                                cache.clusters = _.filter(cache.clusters, function (clusterId) {
                                                    return _.find(clusters, {_id: clusterId});
                                                });
                                            });

                                            _.forEach(igfss, function (igfs) {
                                                // Remove deleted caches.
                                                igfs.clusters = _.filter(igfs.clusters, function (clusterId) {
                                                    return _.find(clusters, {_id: clusterId});
                                                });
                                            });

                                            _.forEach(clusters, function (cluster) {
                                                // Remove deleted caches.
                                                cluster.caches = _.filter(cluster.caches, function (cacheId) {
                                                    return _.find(caches, {_id: cacheId});
                                                });

                                                // Remove deleted IGFS.
                                                cluster.igfss = _.filter(cluster.igfss, function (igfsId) {
                                                    return _.findIndex(igfss, function (igfs) {
                                                            return igfs._id.equals(igfsId);
                                                        }) >= 0;
                                                });
                                            });

                                            res.json({
                                                spaces: spaces,
                                                caches: caches,
                                                igfss: igfss,
                                                clusters: clusters
                                            });
                                        }
                                    });
                            });
                        }
                    });
                }
            });
        });

        /**
         * Save cluster.
         */
        router.post('/save', function (req, res) {
            var params = req.body;
            var clusterId = params._id;
            var caches = params.caches;

            if (params._id)
                mongo.Cluster.update({_id: params._id}, params, {upsert: true}, function (err) {
                    if (mongo.processed(err, res))
                        mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, function (err) {
                            if (mongo.processed(err, res)) {
                                mongo.Cache.update({_id: {$nin: caches}}, {$pull: {clusters: clusterId}}, {multi: true}, function (err) {
                                    if (mongo.processed(err, res))
                                        res.send(params._id);
                                });
                            }
                        });
                });
            else {
                mongo.Cluster.findOne({space: params.space, name: params.name}, function (err, cluster) {
                    if (mongo.processed(err, res)) {
                        if (cluster)
                            return res.status(500).send('Cluster with name: "' + cluster.name + '" already exist.');

                        (new mongo.Cluster(params)).save(function (err, cluster) {
                            if (mongo.processed(err, res)) {
                                clusterId = cluster._id;

                                mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, function (err) {
                                    if (mongo.processed(err, res))
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
        router.post('/remove', function (req, res) {
            mongo.Cluster.remove(req.body, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        });

        /**
         * Remove all clusters.
         */
        router.post('/remove/all', function (req, res) {
            var user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
                if (mongo.processed(err, res)) {
                    var space_ids = spaces.map(function (value) {
                        return value._id;
                    });

                    mongo.Cluster.remove({space: {$in: space_ids}}, function (err) {
                        if (err)
                            return res.status(500).send(err.message);

                        mongo.Cache.update({space: {$in: space_ids}}, {clusters: []}, {multi: true}, function (err) {
                            if (mongo.processed(err, res))
                                mongo.Igfs.update({space: {$in: space_ids}}, {clusters: []}, {multi: true}, function (err) {
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
