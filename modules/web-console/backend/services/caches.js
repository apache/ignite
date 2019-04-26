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

const _ = require('lodash');

// Fire me up!

module.exports = {
    implements: 'services/caches',
    inject: ['mongo', 'services/spaces', 'errors']
};

/**
 * @param mongo
 * @param {SpacesService} spacesService
 * @param errors
 * @returns {CachesService}
 */
module.exports.factory = (mongo, spacesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     *
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = (result) => ({rowsAffected: result.n});

    /**
     * Update existing cache.
     *
     * @param {Object} cache - The cache for updating.
     * @returns {Promise.<mongo.ObjectId>} that resolves cache id.
     */
    const update = (cache) => {
        const cacheId = cache._id;

        return mongo.Cache.updateOne({_id: cacheId}, cache, {upsert: true}).exec()
            .then(() => mongo.Cluster.updateMany({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}).exec())
            .then(() => mongo.Cluster.updateMany({_id: {$nin: cache.clusters}}, {$pull: {caches: cacheId}}).exec())
            .then(() => mongo.DomainModel.updateMany({_id: {$in: cache.domains}}, {$addToSet: {caches: cacheId}}).exec())
            .then(() => mongo.DomainModel.updateMany({_id: {$nin: cache.domains}}, {$pull: {caches: cacheId}}).exec())
            .then(() => cache)
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cache with name: "' + cache.name + '" already exist.');
                else
                    throw err;
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
                mongo.Cluster.updateMany({_id: {$in: savedCache.clusters}}, {$addToSet: {caches: savedCache._id}}).exec()
                    .then(() => mongo.DomainModel.updateMany({_id: {$in: savedCache.domains}}, {$addToSet: {caches: savedCache._id}}).exec())
                    .then(() => savedCache)
            )
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Cache with name: "' + cache.name + '" already exist.');
                else
                    throw err;
            });
    };

    /**
     * Remove all caches by space ids.
     *
     * @param {Number[]} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cluster.updateMany({space: {$in: spaceIds}}, {caches: []}).exec()
            .then(() => mongo.Cluster.updateMany({space: {$in: spaceIds}}, {$pull: {checkpointSpi: {kind: 'Cache'}}}).exec())
            .then(() => mongo.DomainModel.updateMany({space: {$in: spaceIds}}, {caches: []}).exec())
            .then(() => mongo.Cache.deleteMany({space: {$in: spaceIds}}).exec());
    };

    /**
     * Service for manipulate Cache entities.
     */
    class CachesService {
        static shortList(userId, demo, clusterId) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => mongo.Cache.find({space: {$in: spaceIds}, clusters: clusterId }).select('name cacheMode atomicityMode backups').lean().exec());
        }

        static get(userId, demo, _id) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => mongo.Cache.findOne({space: {$in: spaceIds}, _id}).lean().exec());
        }

        static upsertBasic(cache) {
            if (_.isNil(cache._id))
                return Promise.reject(new errors.IllegalArgumentException('Cache id can not be undefined or null'));

            const query = _.pick(cache, ['space', '_id']);
            const newDoc = _.pick(cache, ['space', '_id', 'name', 'cacheMode', 'atomicityMode', 'backups', 'clusters']);

            return mongo.Cache.updateOne(query, {$set: newDoc}, {upsert: true}).exec()
                .catch((err) => {
                    if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                        throw new errors.DuplicateKeyException(`Cache with name: "${cache.name}" already exist.`);

                    throw err;
                })
                .then((updated) => {
                    if (updated.nModified === 0)
                        return mongo.Cache.updateOne(query, {$set: cache}, {upsert: true}).exec();

                    return updated;
                });
        }

        static upsert(cache) {
            if (_.isNil(cache._id))
                return Promise.reject(new errors.IllegalArgumentException('Cache id can not be undefined or null'));

            const query = _.pick(cache, ['space', '_id']);

            return mongo.Cache.updateOne(query, {$set: cache}, {upsert: true}).exec()
                .then(() => mongo.DomainModel.updateMany({_id: {$in: cache.domains}}, {$addToSet: {caches: cache._id}}).exec())
                .then(() => mongo.DomainModel.updateMany({_id: {$nin: cache.domains}}, {$pull: {caches: cache._id}}).exec())
                .catch((err) => {
                    if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                        throw new errors.DuplicateKeyException(`Cache with name: "${cache.name}" already exist.`);

                    throw err;
                });
        }

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
         * Remove caches.
         *
         * @param {Array.<String>|String} ids - The cache ids for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(ids) {
            if (_.isNil(ids))
                return Promise.reject(new errors.IllegalArgumentException('Cache id can not be undefined or null'));

            ids = _.castArray(ids);

            if (_.isEmpty(ids))
                return Promise.resolve({rowsAffected: 0});

            return mongo.Cluster.updateMany({caches: {$in: ids}}, {$pull: {caches: {$in: ids}}}).exec()
                .then(() => mongo.Cluster.updateMany({}, {$pull: {checkpointSpi: {kind: 'Cache', Cache: {cache: {$in: ids}}}}}).exec())
                // TODO WC-201 fix cleanup of cache on deletion for cluster service configuration.
                // .then(() => mongo.Cluster.updateMany({'serviceConfigurations.cache': cacheId}, {$unset: {'serviceConfigurations.$.cache': ''}}).exec())
                .then(() => mongo.DomainModel.updateMany({caches: {$in: ids}}, {$pull: {caches: {$in: ids}}}).exec())
                .then(() => mongo.Cache.deleteMany({_id: {$in: ids}}).exec())
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
            return spacesService.spaceIds(userId, demo)
                .then(removeAllBySpaces)
                .then(convertRemoveStatus);
        }
    }

    return CachesService;
};
