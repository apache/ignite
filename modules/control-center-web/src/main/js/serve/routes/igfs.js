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
    implements: 'igfs-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function(_, express, mongo) {
    return new Promise((resolve) => {
        const router = new express.Router();

        /**
         * Get spaces and IGFSs accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const result = {};

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: req.currentUserId()}, {usedBy: {$elemMatch: {account: req.currentUserId()}}}]}).exec()
                .then((spaces) => {
                    result.spaces = spaces;
                    result.spacesIds = spaces.map((value) => value._id);

                    return mongo.Cluster.find({space: {$in: result.spacesIds}}, '_id name').sort('name').exec();
                })
                .then(clusters => {
                    result.clusters = clusters;

                    return  mongo.Igfs.find({space: {$in: result.spacesIds}}).sort('name').exec();
                })
                .then(igfss => {
                    _.forEach(igfss, (igfs) => {
                        // Remove deleted clusters.
                        igfs.clusters = _.filter(igfs.clusters, (clusterId) => {
                            return _.findIndex(result.clusters, (cluster) => cluster._id.equals(clusterId)) >= 0;
                        });
                    });

                    res.json({
                        spaces: result.spaces,
                        clusters: result.clusters.map((cluster) => ({value: cluster._id, label: cluster.name})),
                        igfss
                    });
                })
                .catch((err) => {
                    // TODO IGNITE-843 Send error to admin
                    res.status(500).send(err.message);
                });
        });

        /**
         * Save IGFS.
         */
        router.post('/save', (req, res) => {
            const params = req.body;
            const clusters = params.clusters;
            let igfsId = params._id;

            if (params._id) {
                mongo.Igfs.update({_id: igfsId}, params, {upsert: true}, (errIgfs) => {
                    if (mongo.processed(errIgfs, res)) {
                        mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}, (errClusterAdd) => {
                            if (mongo.processed(errClusterAdd, res)) {
                                mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {igfss: igfsId}}, {multi: true}, (errClusterPull) => {
                                    if (mongo.processed(errClusterPull, res))
                                        res.send(params._id);
                                });
                            }
                        });
                    }
                });
            }
            else {
                mongo.Igfs.findOne({space: params.space, name: params.name}, (errIgfsFind, igfsFound) => {
                    if (mongo.processed(errIgfsFind, res)) {
                        if (igfsFound)
                            return res.status(500).send('IGFS with name: "' + igfsFound.name + '" already exist.');

                        (new mongo.Igfs(params)).save((errIgfsSave, igfs) => {
                            if (mongo.processed(errIgfsSave, res)) {
                                igfsId = igfs._id;

                                mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}, (errCluster) => {
                                    if (mongo.processed(errCluster, res))
                                        res.send(igfsId);
                                });
                            }
                        });
                    }
                });
            }
        });

        /**
         * Remove IGFS by ._id.
         */
        router.post('/remove', (req, res) => {
            mongo.Igfs.remove(req.body, (err) => {
                if (mongo.processed(err, res))
                    res.sendStatus(200);
            });
        });

        /**
         * Remove all IGFSs.
         */
        router.post('/remove/all', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    mongo.Igfs.remove({space: {$in: space_ids}}, (errIgfs) => {
                        if (mongo.processed(errIgfs, res)) {
                            mongo.Cluster.update({space: {$in: space_ids}}, {igfss: []}, {multi: true}, (errCluster) => {
                                if (mongo.processed(errCluster, res))
                                    res.sendStatus(200);
                            });
                        }
                    });
                }
            });
        });

        resolve(router);
    });
};

