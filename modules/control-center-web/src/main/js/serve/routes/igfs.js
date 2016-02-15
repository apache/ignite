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
                .then((clusters) => {
                    result.clusters = clusters;

                    return mongo.Igfs.find({space: {$in: result.spacesIds}}).sort('name').exec();
                })
                .then((igfss) => {
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
                mongo.Igfs.update({_id: igfsId}, params, {upsert: true}).exec()
                    .then(() => mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}).exec())
                    .then(() => mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {igfss: igfsId}}, {multi: true}).exec())
                    .then(() => res.send(igfsId))
                    .catch((err) => {
                        // TODO IGNITE-843 Send error to admin
                        res.status(500).send(err.message);
                    });
            }
            else {
                mongo.Igfs.findOne({space: params.space, name: params.name}).exec()
                    .then((igfs) => {
                        if (igfs)
                            throw new Error('IGFS with name: "' + igfs + '" already exist.');

                        return (new mongo.Igfs(params)).save();
                    })
                    .then((igfs) => {
                        igfsId = igfs._id;

                        return mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true});
                    })
                    .then(() => res.send(igfsId))
                    .catch((err) => {
                        // TODO IGNITE-843 Send error to admin
                        res.status(500).send(err.message);
                    });
            }
        });

        /**
         * Remove IGFS by ._id.
         */
        router.post('/remove', (req, res) => {
            const igfsId = req.body;

            mongo.Cluster.update({igfss: {$in: [igfsId]}}, {$pull: {igfss: igfsId}}, {multi: true}).exec()
                .then(mongo.Igfs.remove(igfsId))
                .then(() => res.sendStatus(200))
                .catch((err) => {
                    // TODO IGNITE-843 Send error to admin
                    res.status(500).send(err.message);
                });
        });

        /**
         * Remove all IGFSs.
         */
        router.post('/remove/all', (req, res) => {
            const userId = req.currentUserId();
            let spacesIds = [];

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: userId}, {usedBy: {$elemMatch: {account: userId}}}]})
                .then((spaces) => {
                    spacesIds = spaces.map((value) => value._id);

                    return mongo.Igfs.remove({space: {$in: spacesIds}});
                })
                .then(() => mongo.Cluster.update({space: {$in: spacesIds}}, {igfss: []}, {multi: true}))
                .then(() => res.sendStatus(200))
                .catch((err) => {
                    // TODO IGNITE-843 Send error to admin
                    res.status(500).send(err.message);
                });
        });

        resolve(router);
    });
};

