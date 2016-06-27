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
    implements: 'services/cacheService',
    inject: ['require(lodash)',
        'require(express)',
        'mongo',
        'errors']
};

module.exports.factory = (_, express, mongo, errors) => {

    class CacheService {
                
        static update(cache) {
            const cacheId = cache._id;

            return mongo.Cache.update({_id: cacheId}, cache, {upsert: true}).exec()
                .then(() => mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cluster.update({_id: {$nin: cache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.DomainModel.update({_id: {$nin: cache.domains}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => cacheId);
        }

        static create(cache) {
            // TODO: Replace to mongo.Cache.create()
            return (new mongo.Cache(cache)).save()
                .then((cache) =>
                    mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cache._id}}, {multi: true}).exec()
                        .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cache._id}}, {multi: true}).exec())
                        .then(() => cache._id)
                )
        }

        static merge(cache) {
            return this.loadByName(cache)
                .then((existingCache) => {
                    const cacheId = cache._id;

                    if (existingCache && cacheId !== existingCache._id.toString())
                        throw new errors.DuplicateKeyException('Cache with name: "' + existingCache.name + '" already exist.');

                    if (cacheId) {
                        return this.update(cache);
                    }

                    return this.create(cache);
                });
        }

        static loadByName(cache) {
            return mongo.Cache.findOne({space: cache.space, name: cache.name}).exec();
        };

        static listByUser(userId, demo) {
            const result = {};
            let spaceIds = [];

            // Get owned space and all accessed space.
            return mongo.spaces(userId, demo)
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

                    return result;
                })
        }

        static remove(cache) {
            const cacheId = cache._id;

            return mongo.Cluster.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => mongo.DomainModel.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cache.remove(cache).exec())
                .then(() => ({}))
        };

        static removeAll(user, demo) {
            return mongo.spaceIds(user, demo)
                .then((spaceIds) =>
                    mongo.Cluster.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec()
                        .then(() => mongo.DomainModel.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec())
                        .then(() => mongo.Cache.remove({space: {$in: spaceIds}}).exec())
                        .then(() => ({}))
                );
        };
    }

    return CacheService;
};
