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
    inject: ['require(lodash)', 'require(express)', 'mongo', 'services/cacheService']
};

module.exports.factory = function(_, express, mongo, cacheService) {
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
            let spaceIds = [];

            // Get owned space and all accessed space.
            mongo.spaces(req.currentUserId(), req.header('IgniteDemoMode'))
                .then((spaces) => {
                    result.spaces = spaces;
                    spaceIds = spaces.map((space) => space._id);

                    return mongo.Cluster.find({space: {$in: spaceIds}}).sort('name').lean().exec();
                })
                .then((clusters) => {
                    result.clusters = clusters;

                    return mongo.DomainModel.find({space: {$in: spaceIds}}).sort('name').lean().exec();
                })
                .then((domains) => {
                    result.domains = domains;

                    return mongo.Cache.find({space: {$in: spaceIds}}).sort('name').lean().exec();
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
            const cache = req.body;

            cacheService.save(cache)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove cache by ._id.
         */
        router.post('/remove', (req, res) => {
            const params = req.body;
            const cacheId = params._id;

            mongo.Cluster.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => mongo.DomainModel.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cache.remove(params).exec())
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all caches.
         */
        router.post('/remove/all', (req, res) => {
            mongo.spaceIds(req.currentUserId(), req.header('IgniteDemoMode'))
                .then((spaceIds) =>
                    mongo.Cluster.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec()
                        .then(() => mongo.DomainModel.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec())
                        .then(() => mongo.Cache.remove({space: {$in: spaceIds}}).exec())
                )
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};

