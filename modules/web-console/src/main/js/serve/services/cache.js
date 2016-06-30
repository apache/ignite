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
    implements: 'services/cache',
    inject: ['require(lodash)',
        'mongo',
        'errors']
};

module.exports.factory = (_, mongo, errors) => {


    /**
     * Convert remove status operation to own presentation.
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing cache
     * @param {Object} cache - The cache for updating
     * @returns {Promise.<Integer>} that resolves cache id
     */
    const update = (cache) => {
        const cacheId = cache._id;

        return mongo.Cache.update({_id: cacheId}, cache, {upsert: true}).exec()
            .then(() => mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.Cluster.update({_id: {$nin: cache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.DomainModel.update({_id: {$nin: cache.domains}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
            .then(() => cacheId);
    };

    /**
     * Create new cache.
     * @param {Object} cache - The cache for creation.
     * @returns {Promise.<Integer>} that resolves cache id.
     */
    const create = (cache) => {
        return mongo.Cache.create(cache)
            .then((createdCache) =>
                mongo.Cluster.update({_id: {$in: createdCache.clusters}}, {$addToSet: {caches: createdCache._id}}, {multi: true}).exec()
                    .then(() => mongo.DomainModel.update({_id: {$in: createdCache.domains}}, {$addToSet: {caches: createdCache._id}}, {multi: true}).exec())
                    .then(() => createdCache._id)
            )
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cache with name: "' + cache.name + '" already exist.');
            });
    };

    /**
     * Remove all caches by space ids.
     * @param {Integer[]} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<Integer>} that resolves number of affected rows.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cluster.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec()
            .then(() => mongo.DomainModel.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec())
            .then(() => mongo.Cache.remove({space: {$in: spaceIds}}).exec())
            .then(convertRemoveStatus)
    };

    /**
     * Service for manipulate Cache entities.
     */
    class CacheService {
        /**
         * Create or update cache.
         * @param {Object} cache - The cache.
         * @returns {Promise.<Integer>} that resolves cache id of merge operation.
         */
        static merge(cache) {
            if (cache._id)
                return update(cache);

            return create(cache);
        }

        /**
         * Get caches and linked objects by user.
         * @param {Integer} userId - The user id that own caches.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<[mongo.Cache[], mongo.Cluster[], mongo.DomainModel[], mongo.Space[]]>} - contains requested caches and array of linked objects: clusters, domains, spaces.
         */
        static listByUser(userId, demo) {
            // Get owned space and all accessed space.
            return mongo.spaces(userId, demo)
                .then((spaces) => {
                    const spaceIds = spaces.map((space) => space._id);

                    return Promise.all([
                        mongo.Cluster.find({space: {$in: spaceIds}}).sort('name').lean().exec(),
                        mongo.DomainModel.find({space: {$in: spaceIds}}).sort('name').lean().exec(),
                        mongo.Cache.find({space: {$in: spaceIds}}).sort('name').lean().exec()
                    ])
                        .then(([clusters, domains, caches]) => ({caches, clusters, domains, spaces}));
                });
        }

        /**
         * Remove cache.
         * @param {Object} cache - The cache object with _id property.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(cache) {
            const cacheId = cache._id;

            return mongo.Cluster.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => mongo.DomainModel.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cache.remove(cache).exec())
                .then(convertRemoveStatus);
        }

        /**
         * Remove all caches by user.
         * @param {Integer} userId - The user id that own caches.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static removeAll(userId, demo) {
            return mongo.spaceIds(userId, demo)
                .then(removeAllBySpaces);
        }
    }

    return CacheService;
};
