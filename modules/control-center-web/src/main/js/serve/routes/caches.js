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
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get spaces and caches accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const result = {};
            let spacesIds = [];

            // Get owned space and all accessed space.
            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    result.spaces = spaces;
                    spacesIds = mongo.spacesIds(spaces);

                    return mongo.Cluster.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((clusters) => {
                    result.clusters = clusters;

                    return mongo.DomainModel.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((domains) => {
                    result.domains = domains;

                    return mongo.Cache.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((caches) => {
                    result.caches = caches;

                    res.json(result);
                })
                .catch((err) => mongo.handleError(res, err));
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
                mongo.Cache.update({_id: cacheId}, params, {upsert: true}).exec()
                    .then(() => mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                    .then(() => mongo.Cluster.update({_id: {$nin: clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                    .then(() => mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                    .then(() => mongo.DomainModel.update({_id: {$nin: domains}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                    .then(() => res.send(cacheId))
                    .catch((err) => mongo.handleError(res, err));
            }
            else {
                mongo.Cache.findOne({space: params.space, name: params.name}).exec()
                    .then((cache) => {
                        if (cache)
                            throw new Error('Cache with name: "' + cache.name + '" already exist.');

                        return (new mongo.Cache(params)).save();
                    })
                    .then((cache) => {
                        cacheId = cache._id;

                        return mongo.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec();
                    })
                    .then(() => mongo.DomainModel.update({_id: {$in: domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                    .then(() => res.send(cacheId))
                    .catch((err) => mongo.handleError(res, err));
            }
        });

        /**
         * Remove cache by ._id.
         */
        router.post('/remove', (req, res) => {
            const params = req.body;
            const cacheId = params._id;

            mongo.Cluster.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => mongo.DomainModel.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cache.remove(params))
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all caches.
         */
        router.post('/remove/all', (req, res) => {
            let spacesIds = [];

            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    spacesIds = mongo.spacesIds(spaces);

                    return mongo.Cluster.update({space: {$in: spacesIds}}, {caches: []}, {multi: true});
                })
                .then(() => mongo.DomainModel.update({space: {$in: spacesIds}}, {caches: []}, {multi: true}))
                .then(() => mongo.Cache.remove({space: {$in: spacesIds}}))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};

