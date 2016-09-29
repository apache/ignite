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
    implements: 'services/domains',
    inject: ['require(lodash)', 'mongo', 'services/spaces', 'services/caches', 'errors']
};

/**
 * @param _
 * @param mongo
 * @param {SpacesService} spacesService
 * @param {CachesService} cachesService
 * @param errors
 * @returns {DomainsService}
 */
module.exports.factory = (_, mongo, spacesService, cachesService, errors) => {
    /**
     * Convert remove status operation to own presentation.
     * @param {RemoveResult} result - The results of remove operation.
     */
    const convertRemoveStatus = ({result}) => ({rowsAffected: result.n});

    const _updateCacheStore = (cacheStoreChanges) =>
        Promise.all(_.map(cacheStoreChanges, (change) => mongo.Cache.update({_id: {$eq: change.cacheId}}, change.change, {}).exec()));

    /**
     * Update existing domain
     * @param {Object} domain - The domain for updating
     * @param savedDomains List of saved domains.
     * @returns {Promise.<mongo.ObjectId>} that resolves domain id
     */
    const update = (domain, savedDomains) => {
        const domainId = domain._id;

        return mongo.DomainModel.update({_id: domainId}, domain, {upsert: true}).exec()
            .then(() => mongo.Cache.update({_id: {$in: domain.caches}}, {$addToSet: {domains: domainId}}, {multi: true}).exec())
            .then(() => mongo.Cache.update({_id: {$nin: domain.caches}}, {$pull: {domains: domainId}}, {multi: true}).exec())
            .then(() => {
                savedDomains.push(domain);

                return _updateCacheStore(domain.cacheStoreChanges);
            })
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_UPDATE_ERROR || err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Domain model with value type: "' + domain.valueType + '" already exist.');
            });
    };

    /**
     * Create new domain.
     * @param {Object} domain - The domain for creation.
     * @param savedDomains List of saved domains.
     * @returns {Promise.<mongo.ObjectId>} that resolves cluster id.
     */
    const create = (domain, savedDomains) => {
        return mongo.DomainModel.create(domain)
            .then((createdDomain) => {
                savedDomains.push(createdDomain);

                return mongo.Cache.update({_id: {$in: domain.caches}}, {$addToSet: {domains: createdDomain._id}}, {multi: true}).exec()
                    .then(() => _updateCacheStore(domain.cacheStoreChanges));
            })
            .catch((err) => {
                if (err.code === mongo.errCodes.DUPLICATE_KEY_ERROR)
                    throw new errors.DuplicateKeyException('Domain model with value type: "' + domain.valueType + '" already exist.');
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
     * @param {Array.<Number>} spaceIds - The space ids for cache deletion.
     * @returns {Promise.<RemoveResult>} - that resolves results of remove operation.
     */
    const removeAllBySpaces = (spaceIds) => {
        return mongo.Cache.update({space: {$in: spaceIds}}, {domains: []}, {multi: true}).exec()
            .then(() => mongo.DomainModel.remove({space: {$in: spaceIds}}).exec());
    };

    class DomainsService {
        /**
         * Batch merging domains.
         * @param {Array.<mongo.DomainModel>} domains
         */
        static batchMerge(domains) {
            return _save(domains);
        }

        /**
         * Get domain and linked objects by space.
         * @param {mongo.ObjectId|String} spaceIds - The space id that own domain.
         * @returns {Promise.<[mongo.Cache[], mongo.Cluster[], mongo.DomainModel[], mongo.Space[]]>}
         *      contains requested domains and array of linked objects: caches, spaces.
         */
        static listBySpaces(spaceIds) {
            return mongo.DomainModel.find({space: {$in: spaceIds}}).sort('valueType').lean().exec();
        }

        /**
         * Remove domain.
         * @param {mongo.ObjectId|String} domainId - The domain id for remove.
         * @returns {Promise.<{rowsAffected}>} - The number of affected rows.
         */
        static remove(domainId) {
            if (_.isNil(domainId))
                return Promise.reject(new errors.IllegalArgumentException('Domain id can not be undefined or null'));

            return mongo.Cache.update({domains: {$in: [domainId]}}, {$pull: {domains: domainId}}, {multi: true}).exec()
                .then(() => mongo.DomainModel.remove({_id: domainId}).exec())
                .then(convertRemoveStatus);
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
