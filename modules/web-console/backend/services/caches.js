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
    implements: 'services/caches',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spaceService
 * @param errors
 * @returns {CachesService}
 */
module.exports.factory = (_, mongo, spaceService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     *
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    /**
     * Update existing cache.
     *
     * @param {Object} cache - The cache for updating.
     * @returns {Promise.<mongo.ObjectId>} that resolves cache id.
     */
    const update = (cache) => {
        const cacheId = cache._id;

        return mongo.Cache.update({_id: cacheId}, cache, {upsert: true}).exec()
            .then(() => mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.Cluster.update({_id: {$nin: cache.clusters}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.DomainModel.update({_id: {$in: cache.domains}}, {$addToSet: {caches: cacheId}}, {multi: true}).exec())
            .then(() => mongo.DomainModel.update({_id: {$nin: cache.domains}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
            .then(() => cache)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cache with name: "' + cache.name + '" already exist.');
            });
    };

    /**
     * Create new cache.
     *
     * @param {Object} cache - The cache for creation.
     * @returns {Promise.<mongo.ObjectId>} that resolves cache id.
     */
    const create = (cache) => {
        return mongo.Cache.create(cache)
            .then((savedCache) =>
                mongo.Cluster.update({_id: {$in: savedCache.clusters}}, {$addToSet: {caches: savedCache._id}}, {multi: true}).exec()
                    .then(() => mongo.DomainModel.update({_id: {$in: savedCache.domains}}, {$addToSet: {caches: savedCache._id}}, {multi: true}).exec())
                    .then(() => savedCache)
            )
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cache with name: "' + cache.name + '" already exist.');
            });
    };

    /**
     * Remove all caches by space ids.
     *
     * @param {Number[]} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cluster.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec()
            .then(() => mongo.Cluster.update({space: {$in: spaceIds}}, {$pull: {checkpointSpi: {kind: 'Cache'}}}, {multi: true}).exec())
            .then(() => mongo.DomainModel.update({space: {$in: spaceIds}}, {caches: []}, {multi: true}).exec())
            .then(() => mongo.Cache.remove({space: {$in: spaceIds}}).exec());
    };

    /**
     * Service for manipulate Cache entities.
     */
    class CachesService {
        /**
         * Create or update cache.
         *
         * @param {Object} cache - The cache.
         * @returns {Promise.<mongo.ObjectId>} that resolves cache id of merge operation.
         */
        static merge(cache) {
            if (cache._id)
                return update(cache);

            return create(cache);
        }

        /**
         * Get caches by spaces.
         *
         * @param {mongo.ObjectId|String} spaceIds - The spaces ids that own caches.
         * @returns {Promise.<mongo.Cache[]>} - contains requested caches.
         */
        static listBySpaces(spaceIds) {
            return mongo.Cache.find({space: {$in: spaceIds}}).sort('name').lean().exec();
        }

        /**
         * Remove cache.
         *
         * @param {mongo.ObjectId|String} cacheId - The cache id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(cacheId) {
            if (_.isNil(cacheId))
                return Promise.reject(new errors.IllegalArgumentException('Cache id can not be undefined or null'));

            return mongo.Cluster.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec()
                .then(() => mongo.Cluster.update({}, {$pull: {checkpointSpi: {kind: 'Cache', Cache: {cache: cacheId}}}}, {multi: true}).exec())
                .then(() => mongo.DomainModel.update({caches: {$in: [cacheId]}}, {$pull: {caches: cacheId}}, {multi: true}).exec())
                .then(() => mongo.Cache.remove({_id: cacheId}).exec())
                .then(convertRemoveStatus);
        }

        /**
         * Remove all caches by user.
         *
         * @param {mongo.ObjectId|String} userId - The user id that own caches.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static removeAll(userId, demo) {
            return spaceService.spaceIds(userId, demo)
                .then(removeAllBySpaces)
                .then(convertRemoveStatus);
        }
    }

    return CachesService;
};
