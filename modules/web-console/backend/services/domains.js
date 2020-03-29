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
    implements: 'services/domains',
    inject: ['mongo', 'services/spaces', 'services/caches', 'errors']
};

/**
 * @param mongo
 * @param {SpacesService} spacesService
 * @param {CachesService} cachesService
 * @param errors
 * @returns {DomainsService}
 */
module.exports.factory = (mongo, spacesService, cachesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     *
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = (result) => ({rowsAffected: result.n});

    const _updateCacheStore = (cacheStoreChanges) =>
        Promise.all(_.map(cacheStoreChanges, (change) => mongo.Cache.updateOne({_id: {$eq: change.cacheId}}, change.change, {}).exec()));

    /**
     * Update existing domain.
     *
     * @param {Object} domain - The domain for updating
     * @param savedDomains List of saved domains.
     * @returns {Promise.<mongo.ObjectId>} that resolves domain id
     */
    const update = (domain, savedDomains) => {
        const domainId = domain._id;

        return mongo.DomainModel.updateOne({_id: domainId}, domain, {upsert: true}).exec()
            .then(() => mongo.Cache.updateMany({_id: {$in: domain.caches}}, {$addToSet: {domains: domainId}}).exec())
            .then(() => mongo.Cache.updateMany({_id: {$nin: domain.caches}}, {$pull: {domains: domainId}}).exec())
            .then(() => {
                savedDomains.push(domain);

                return _updateCacheStore(domain.cacheStoreChanges);
            })
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Domain model with value type: "' + domain.valueType + '" already exist.');
                else
                    throw err;
            });
    };

    /**
     * Create new domain.
     *
     * @param {Object} domain - The domain for creation.
     * @param savedDomains List of saved domains.
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id.
     */
    const create = (domain, savedDomains) => {
        return mongo.DomainModel.create(domain)
            .then((createdDomain) => {
                savedDomains.push(createdDomain);

                return mongo.Cache.updateMany({_id: {$in: domain.caches}}, {$addToSet: {domains: createdDomain._id}}).exec()
                    .then(() => _updateCacheStore(domain.cacheStoreChanges));
            })
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Domain model with value type: "' + domain.valueType + '" already exist.');
                else
                    throw err;
            });
    };

    const _saveDomainModel = (domain, savedDomains) => {
        const domainId = domain._id;

        if (domainId)
            return update(domain, savedDomains);

        return create(domain, savedDomains);
    };

    const _save = (domains) => {
        if (_.isEmpty(domains))
            throw new errors.IllegalArgumentException('Nothing to save!');

        const savedDomains = [];
        const generatedCaches = [];

        const promises = _.map(domains, (domain) => {
            if (domain.newCache) {
                return mongo.Cache.findOne({space: domain.space, name: domain.newCache.name}).exec()
                    .then((cache) => {
                        if (cache)
                            return Promise.resolve(cache);

                        // If cache not found, then create it and associate with domain model.
                        const newCache = domain.newCache;
                        newCache.space = domain.space;

                        return cachesService.merge(newCache);
                    })
                    .then((cache) => {
                        domain.caches = [cache._id];

                        generatedCaches.push(cache);

                        return _saveDomainModel(domain, savedDomains);
                    });
            }

            return _saveDomainModel(domain, savedDomains);
        });

        return Promise.all(promises).then(() => ({savedDomains, generatedCaches}));
    };

    /**
     * Remove all caches by space ids.
     *
     * @param {Array.<Number>} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cache.updateMany({space: {$in: spaceIds}}, {domains: []}).exec()
            .then(() => mongo.DomainModel.deleteMany({space: {$in: spaceIds}}).exec());
    };

    class DomainsService {
        static shortList(userId, demo, clusterId) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => {
                    const sIds = _.map(spaceIds, (spaceId) => mongo.ObjectId(spaceId));

                    return mongo.DomainModel.aggregate([
                        {$match: {space: {$in: sIds}, clusters: mongo.ObjectId(clusterId)}},
                        {$project: {
                            keyType: 1,
                            valueType: 1,
                            queryMetadata: 1,
                            hasIndex: {
                                $or: [
                                    {
                                        $and: [
                                            {$eq: ['$queryMetadata', 'Annotations']},
                                            {
                                                $or: [
                                                    {$eq: ['$generatePojo', false]},
                                                    {
                                                        $and: [
                                                            {$eq: ['$databaseSchema', '']},
                                                            {$eq: ['$databaseTable', '']}
                                                        ]
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    {$gt: [{$size: {$ifNull: ['$keyFields', []]}}, 0]}
                                ]
                            }
                        }}
                    ]).exec();
                });
        }

        static get(userId, demo, _id) {
            return spacesService.spaceIds(userId, demo)
                .then((spaceIds) => mongo.DomainModel.findOne({space: {$in: spaceIds}, _id}).lean().exec());
        }

        static upsert(model) {
            if (_.isNil(model._id))
                return Promise.reject(new errors.IllegalArgumentException('Model id can not be undefined or null'));

            const query = _.pick(model, ['space', '_id']);

            return mongo.DomainModel.updateOne(query, {$set: model}, {upsert: true}).exec()
                .then(() => mongo.Cache.updateMany({_id: {$in: model.caches}}, {$addToSet: {domains: model._id}}).exec())
                .then(() => mongo.Cache.updateMany({_id: {$nin: model.caches}}, {$pull: {domains: model._id}}).exec())
                .then(() => _updateCacheStore(model.cacheStoreChanges))
                .catch((err) => {
                    if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                        throw new errors.DuplicateKeyException(`Model with value type: "${model.valueType}" already exist.`);

                    throw err;
                });
        }

        /**
         * Remove model.
         *
         * @param {mongo.ObjectId|String} ids - The model id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(ids) {
            if (_.isNil(ids))
                return Promise.reject(new errors.IllegalArgumentException('Model id can not be undefined or null'));

            ids = _.castArray(ids);

            if (_.isEmpty(ids))
                return Promise.resolve({rowsAffected: 0});

            return mongo.Cache.updateMany({domains: {$in: ids}}, {$pull: {domains: ids}}).exec()
                .then(() => mongo.Cluster.updateMany({models: {$in: ids}}, {$pull: {models: ids}}).exec())
                .then(() => mongo.DomainModel.deleteMany({_id: {$in: ids}}).exec())
                .then(convertRemoveStatus);
        }

        /**
         * Batch merging domains.
         *
         * @param {Array.<mongo.DomainModel>} domains
         */
        static batchMerge(domains) {
            return _save(domains);
        }

        /**
         * Get domain and linked objects by space.
         *
         * @param {mongo.ObjectId|String} spaceIds - The space id that own domain.
         * @returns {Promise.<Array.<mongo.DomainModel>>} contains requested domains.
         */
        static listBySpaces(spaceIds) {
            return mongo.DomainModel.find({space: {$in: spaceIds}}).sort('valueType').lean().exec();
        }

        /**
         * Remove all domains by user.
         * @param {mongo.ObjectId|String} userId - The user id that own domain.
         * @param {Boolean} demo - The flag indicates that need lookup in demo space.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static removeAll(userId, demo) {
            return spacesService.spaceIds(userId, demo)
                .then(removeAllBySpaces)
                .then(convertRemoveStatus);
        }
    }

    return DomainsService;
};
