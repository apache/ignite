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

// Fire me up!

module.exports = {
    implements: 'domains-routes',
    inject: ['require(lodash)', 'require(express)', 'require(async)', 'mongo']
};

module.exports.factory = function(_, express, async, mongo) {
    return new Promise((resolve) => {
        const router = express.Router();

        /**
         * Get spaces and domain models accessed for user account.
         *
         * @param req Request.
         * @param res Response.
         */
        router.post('/list', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    // Get all clusters for spaces.
                    mongo.Cluster.find({space: {$in: space_ids}}, '_id name').sort('name').exec((errCluster, clusters) => {
                        if (mongo.processed(errCluster, res)) {
                            // Get all caches for spaces.
                            mongo.Cache.find({space: {$in: space_ids}}).sort('name').exec((errCache, caches) => {
                                if (mongo.processed(errCache, res)) {
                                    // Get all domain models for spaces.
                                    mongo.DomainModel.find({space: {$in: space_ids}}).sort('valueType').exec((errDomainModel, domains) => {
                                        if (mongo.processed(errDomainModel, res)) {
                                            _.forEach(caches, (cache) => {
                                                cache.domains = _.filter(cache.domains, (metaId) => {
                                                    return _.find(domains, {_id: metaId});
                                                });
                                            });

                                            // Remove deleted caches.
                                            _.forEach(domains, (domain) => {
                                                domain.caches = _.filter(domain.caches, (cacheId) => {
                                                    return _.find(caches, {_id: cacheId});
                                                });
                                            });

                                            res.json({
                                                spaces,
                                                clusters: clusters.map((cluster) => ({value: cluster._id, label: cluster.name})),
                                                caches,
                                                domains
                                            });
                                        }
                                    });
                                }
                            });
                        }
                    });
                }
            });
        });

        function _saveDomainModel(domain, savedDomains, callback) {
            const caches = domain.caches;
            let domainId = domain._id;

            const cacheStoreChanges = domain.cacheStoreChanges;

            if (domainId) {
                mongo.DomainModel.update({_id: domain._id}, domain, {upsert: true}, (errDomainModel) => {
                    if (errDomainModel)
                        callback(errDomainModel);
                    else {
                        mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainId}}, {multi: true}, (errCacheAdd) => {
                            if (errCacheAdd)
                                callback(errCacheAdd);
                            else {
                                mongo.Cache.update({_id: {$nin: caches}}, {$pull: {domains: domainId}}, {multi: true}, (errCachePull) => {
                                    if (errCachePull)
                                        callback(errCachePull);
                                    else {
                                        savedDomains.push(domain);

                                        _updateCacheStore(cacheStoreChanges, callback);
                                    }
                                });
                            }
                        });
                    }
                });
            }
            else {
                mongo.DomainModel.findOne({space: domain.space, valueType: domain.valueType}, (errDomainModelFind, found) => {
                    if (errDomainModelFind)
                        callback(errDomainModelFind);
                    else if (found)
                        return callback('Domain model with value type: "' + found.valueType + '" already exist.');

                    (new mongo.DomainModel(domain)).save((errDomainModel, domainSaved) => {
                        if (errDomainModel)
                            callback(errDomainModel);
                        else {
                            domainId = domainSaved._id;

                            mongo.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainId}}, {multi: true}, (errCache) => {
                                if (errCache)
                                    callback(errCache);
                                else {
                                    savedDomains.push(domainSaved);

                                    _updateCacheStore(cacheStoreChanges, callback);
                                }
                            });
                        }
                    });
                });
            }
        }

        function _updateCacheStore(cacheStoreChanges, callback) {
            if (cacheStoreChanges && cacheStoreChanges.length > 0) {
                async.forEachOf(cacheStoreChanges, (change, idx, cb) => {
                    mongo.Cache.update({_id: {$eq: change.cacheId}}, change.change, {}, (err) => {
                        if (err)
                            cb(err);
                        else
                            cb();
                    });
                }, callback);
            }
            else
                callback();
        }

        function _save(domains, res) {
            const savedDomains = [];
            const generatedCaches = [];

            if (domains && domains.length > 0) {
                async.forEachOf(domains, (domain, idx, callback) => {
                    if (domain.newCache) {
                        mongo.Cache.findOne({space: domain.space, name: domain.newCache.name}, (errCacheFind, cacheFound) => {
                            if (mongo.processed(errCacheFind, res)) {
                                if (cacheFound) {
                                    // Cache already exists, just save domain model.
                                    domain.caches = [cacheFound._id];

                                    _saveDomainModel(domain, savedDomains, callback);
                                }
                                else {
                                    // If cache not found, then create it and associate with domain model.
                                    const newCache = domain.newCache;
                                    newCache.space = domain.space;

                                    (new mongo.Cache(newCache)).save((errCache, cache) => {
                                        const cacheId = cache._id;

                                        if (mongo.processed(errCache, res)) {
                                            mongo.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, (errCluster) => {
                                                if (mongo.processed(errCluster, res)) {
                                                    domain.caches = [cacheId];
                                                    generatedCaches.push(cache);

                                                    _saveDomainModel(domain, savedDomains, callback);
                                                }
                                            });
                                        }
                                    });
                                }
                            }
                        });
                    }
                    else
                        _saveDomainModel(domain, savedDomains, callback);
                }, (err) => {
                    if (err)
                        res.status(500).send(err.message);
                    else
                        res.send({savedDomains, generatedCaches});
                });
            }
            else
                res.status(500).send('Nothing to save!');
        }

        /**
         * Save domain model.
         */
        router.post('/save', (req, res) => {
            _save([req.body], res);
        });

        /**
         * Batch save domain models.
         */
        router.post('/save/batch', (req, res) => {
            _save(req.body, res);
        });

        /**
         * Remove domain model by ._id.
         */
        router.post('/remove', (req, res) => {
            mongo.DomainModel.remove(req.body, (err) => {
                if (mongo.processed(err, res))
                    res.sendStatus(200);
            });
        });

        /**
         * Remove all domain models.
         */
        router.post('/remove/all', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    mongo.DomainModel.remove({space: {$in: space_ids}}, (errDomainModel) => {
                        if (errDomainModel)
                            return res.status(500).send(errDomainModel.message);

                        mongo.Cache.update({space: {$in: space_ids}}, {domains: []}, {multi: true}, (errCache) => {
                            if (mongo.processed(errCache, res))
                                res.sendStatus(200);
                        });
                    });
                }
            });
        });

        /**
         * Remove all generated demo domain models and caches.
         */
        router.post('/remove/demo', (req, res) => {
            const user_id = req.currentUserId();

            // Get owned space and all accessed space.
            mongo.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, (errSpace, spaces) => {
                if (mongo.processed(errSpace, res)) {
                    const space_ids = spaces.map((value) => value._id);

                    // Remove all demo domain models.
                    mongo.DomainModel.remove({$and: [{space: {$in: space_ids}}, {demo: true}]}, (errDomainModel) => {
                        if (errDomainModel)
                            return res.status(500).send(errDomainModel.message);

                        // Remove all demo caches.
                        mongo.Cache.remove({$and: [{space: {$in: space_ids}}, {demo: true}]}, (errCache) => {
                            if (errCache)
                                return res.status(500).send(errCache.message);

                            res.sendStatus(200);
                        });
                    });
                }
            });
        });

        resolve(router);
    });
};

