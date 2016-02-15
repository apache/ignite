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
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get spaces and clusters accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const result = {};
            let spacesIds = [];

            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    result.spaces = spaces;
                    spacesIds = spaces.map((value) => value._id);

                    return mongo.Cache.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((caches) => {
                    result.caches = caches;

                    return mongo.Igfs.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((igfss) => {
                    result.igfss = igfss;

                    return mongo.Cluster.find({space: {$in: spacesIds}}).sort('name').deepPopulate(mongo.ClusterDefaultPopulate).exec();
                })
                .then((clusters) => {
                    result.clusters = clusters;

                    res.json(result);
                })
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Save cluster.
         */
        router.post('/save', (req, res) => {
            const params = req.body;
            const caches = params.caches;
            const igfss = params.igfss;
            let clusterId = params._id;

            if (params._id) {
                mongo.Cluster.update({_id: params._id}, params, {upsert: true}).exec()
                    .then(() => mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => mongo.Cache.update({_id: {$nin: caches}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => mongo.Igfs.update({_id: {$in: igfss}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => mongo.Igfs.update({_id: {$nin: igfss}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => res.send(clusterId))
                    .catch((err) => mongo.handleError(res, err));
            }
            else {
                mongo.Cluster.findOne({space: params.space, name: params.name}).exec()
                    .then((cluster) => {
                        if (cluster)
                            return res.status(500).send('Cluster with name: "' + cluster.name + '" already exist.');

                        return (new mongo.Cluster(params)).save();
                    })
                    .then((cluster) => {
                        clusterId = cluster._id;

                        return mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec();
                    })
                    .then(() => mongo.Cache.update({_id: {$nin: caches}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => mongo.Igfs.update({_id: {$in: igfss}}, {$addToSet: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => mongo.Igfs.update({_id: {$nin: igfss}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                    .then(() => res.send(clusterId))
                    .catch((err) => mongo.handleError(res, err));
            }
        });

        /**
         * Remove cluster by ._id.
         */
        router.post('/remove', (req, res) => {
            const params = req.body;
            const clusterId = params._id;

            mongo.Cache.update({clusters: {$in: [clusterId]}}, {$pull: {clusters: clusterId}}, {multi: true}).exec()
                .then(() => mongo.Igfs.update({clusters: {$in: [clusterId]}}, {$pull: {clusters: clusterId}}, {multi: true}).exec())
                .then(() => mongo.Cluster.remove(params))
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all clusters.
         */
        router.post('/remove/all', (req, res) => {
            let spacesIds = [];

            // Get owned space and all accessed space.
            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    spacesIds = spaces.map((value) => value._id);

                    return mongo.Cluster.remove({space: {$in: spacesIds}});
                })
                .then(() => mongo.Cache.update({space: {$in: spacesIds}}, {clusters: []}, {multi: true}).exec())
                .then(() => mongo.Igfs.update({space: {$in: spacesIds}}, {clusters: []}, {multi: true}).exec())
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};
