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
    implements: 'domains-routes',
    inject: ['require(lodash)', 'require(express)', 'mongo']
};

module.exports.factory = function(_, express, async, mongo) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get spaces and domain models accessed for user account.
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

                    return mongo.Cluster.find({space: {$in: spacesIds}}, '_id name').sort('name').exec();
                })
                .then((clusters) => {
                    result.clusters = clusters;

                    return mongo.Cache.find({space: {$in: spacesIds}}).sort('name').exec();
                })
                .then((caches) => {
                    result.caches = caches;

                    return mongo.DomainModel.find({space: {$in: spacesIds}}).sort('valueType').exec();
                })
                .then((domains) => {
                    result.domains = domains;

                    res.json(result);
                })
                .catch((err) => mongo.handleError(res, err));
        });

        // function _saveDomainModel(domain, savedDomains, callback) {
        //    const caches = domain.caches;
        //    const cacheStoreChanges = domain.cacheStoreChanges;
        //    let domainId = domain._id;
        //
        //    if (domainId) {
        //        Promise.all([
        //            mongo.DomainModel.update({_id: domain._id}, domain, {upsert: true}).exec(),
        //            mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainId}}, {multi: true}).exec(),
        //            mongo.Cache.update({_id: {$nin: caches}}, {$pull: {domains: domainId}}, {multi: true}).exec()
        //        ]).then(() => {
        //                savedDomains.push(domain);
        //
        //                _updateCacheStore(cacheStoreChanges, callback);
        //        });
        //    }
        //    else {
        //        mongo.DomainModel.findOne({space: domain.space, valueType: domain.valueType}).exec()
        //            .then((found) => {
        //                if (found)
        //                    reject(new Error('Domain model with value type: "' + found.valueType + '" already exist.'));
        //
        //                return (new mongo.DomainModel(domain)).save();
        //            })
        //            .then((domainSaved) => {
        //                savedDomains.push(domainSaved);
        //
        //                return mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainSaved._id}}, {multi: true}).exec();
        //            }).then(() => _updateCacheStore(cacheStoreChanges));
        //    }
        // }

        // function _updateCacheStore(cacheStoreChanges, callback) {
        //    if (cacheStoreChanges && cacheStoreChanges.length > 0) {
        //        async.forEachOf(cacheStoreChanges, (change, idx, cb) => {
        //            mongo.Cache.update({_id: {$eq: change.cacheId}}, change.change, {}, (err) => {
        //                if (err)
        //                    cb(err);
        //                else
        //                    cb();
        //            });
        //        }, callback);
        //    }
        //    else
        //        callback();
        // }

        // function _save(domains, res) {
        //    if (domains && domains.length > 0) {
        //        const savedDomains = [];
        //        const generatedCaches = [];
        //        const promises = [];
        //
        //        _.forEach(domains, (domain) => {
        //            promises.push();
        //        });
        //
        //        Promise.all(promises)
        //            .then(() => res.send({savedDomains, generatedCaches}))
        //            .catch((err) => mongo.handleError(res, err));
        //
        //        //async.forEachOf(domains, (domain, idx, callback) => {
        //        //    if (domain.newCache) {
        //        //        mongo.Cache.findOne({space: domain.space, name: domain.newCache.name}, (errCacheFind, cacheFound) => {
        //        //            if (mongo.processed(errCacheFind, res)) {
        //        //                if (cacheFound) {
        //        //                    // Cache already exists, just save domain model.
        //        //                    domain.caches = [cacheFound._id];
        //        //
        //        //                    _saveDomainModel(domain, savedDomains, callback);
        //        //                }
        //        //                else {
        //        //                    // If cache not found, then create it and associate with domain model.
        //        //                    const newCache = domain.newCache;
        //        //                    newCache.space = domain.space;
        //        //
        //        //                    (new mongo.Cache(newCache)).save((errCache, cache) => {
        //        //                        const cacheId = cache._id;
        //        //
        //        //                        if (mongo.processed(errCache, res)) {
        //        //                            mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errCluster) => {
        //        //                                if (mongo.processed(errCluster, res)) {
        //        //                                    domain.caches = [cacheId];
        //        //                                    generatedCaches.push(cache);
        //        //
        //        //                                    _saveDomainModel(domain, savedDomains, callback);
        //        //                                }
        //        //                            });
        //        //                        }
        //        //                    });
        //        //                }
        //        //            }
        //        //        });
        //        //    }
        //        //    else
        //        //        _saveDomainModel(domain, savedDomains, callback);
        //        //}
        //    }
        //    else
        //        res.status(500).send('Nothing to save!');
        // }

        /**
         * Save domain model.
         */
        router.post('/save', (req, res) => {
            res.status(500).send('Not ready!');
            // _save([req.body], res);
        });

        /**
         * Batch save domain models.
         */
        router.post('/save/batch', (req, res) => {
            res.status(500).send('Not ready!');
            // _save(req.body, res);
        });

        /**
         * Remove domain model by ._id.
         */
        router.post('/remove', (req, res) => {
            mongo.DomainModel.remove(req.body)
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all domain models.
         */
        router.post('/remove/all', (req, res) => {
            let spacesIds = [];

            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    spacesIds = spaces.map((value) => value._id);

                    return mongo.Cache.update({space: {$in: spacesIds}}, {domains: []}, {multi: true}).exec();
                })
                .then(() => mongo.DomainModel.remove({space: {$in: spacesIds}}).exec())
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        /**
         * Remove all generated demo domain models and caches.
         */
        router.post('/remove/demo', (req, res) => {
            let spacesIds = [];

            // TODO IGNITE-843 also remove from links: Cache -> DomainModel ; DomainModel -> Cache; Cluster -> Cache.

            mongo.spaces(req.currentUserId())
                .then((spaces) => {
                    spacesIds = spaces.map((value) => value._id);

                    return mongo.DomainModel.remove({$and: [{space: {$in: spacesIds}}, {demo: true}]});
                })
                .then(() => mongo.Cache.remove({$and: [{space: {$in: spacesIds}}, {demo: true}]}))
                .then(() => res.sendStatus(200))
                .catch((err) => mongo.handleError(res, err));
        });

        factoryResolve(router);
    });
};

