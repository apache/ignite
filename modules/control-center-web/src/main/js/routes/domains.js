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

var async = require('async');
var _ = require('lodash');
var router = require('express').Router();
var db = require('../db');

/**
 * Get spaces and domain models accessed for user account.
 *
 * @param req Request.
 * @param res Response.
 */
router.post('/list', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            // Get all clusters for spaces.
            db.Cluster.find({space: {$in: space_ids}}, '_id name').sort('name').exec(function (err, clusters) {
                if (db.processed(err, res)) {
                    // Get all caches for spaces.
                    db.Cache.find({space: {$in: space_ids}}).sort('name').exec(function (err, caches) {
                        if (db.processed(err, res)) {
                            // Get all domain models for spaces.
                            db.DomainModel.find({space: {$in: space_ids}}).sort('valueType').exec(function (err, domains) {
                                if (db.processed(err, res)) {
                                    _.forEach(caches, function (cache) {
                                        cache.domains = _.filter(cache.domains, function (metaId) {
                                            return _.find(domains, {_id: metaId});
                                        });
                                    });

                                    // Remove deleted caches.
                                    _.forEach(domains, function (domain) {
                                        domain.caches = _.filter(domain.caches, function (cacheId) {
                                            return _.find(caches, {_id: cacheId});
                                        });
                                    });

                                    res.json({
                                        spaces: spaces,
                                        clusters: clusters.map(function (cluster) {
                                            return {value: cluster._id, label: cluster.name};
                                        }),
                                        caches: caches,
                                        domains: domains
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
    var domainId = domain._id;
    var caches = domain.caches;

    var cacheStoreChanges = domain.cacheStoreChanges;

    if (domainId)
        db.DomainModel.update({_id: domain._id}, domain, {upsert: true}, function (err) {
            if (err)
                callback(err);
            else
                db.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainId}}, {multi: true}, function (err) {
                    if (err)
                        callback(err);
                    else
                        db.Cache.update({_id: {$nin: caches}}, {$pull: {domains: domainId}}, {multi: true}, function (err) {
                            if (err)
                                callback(err);
                            else {
                                savedDomains.push(domain);

                                _updateCacheStore(cacheStoreChanges, callback);
                            }
                        });
                });
        });
    else
        db.DomainModel.findOne({space: domain.space, valueType: domain.valueType}, function (err, found) {
            if (err)
                callback(err);
            else if (found)
                return callback('Domain model with value type: "' + found.valueType + '" already exist.');

            (new db.DomainModel(domain)).save(function (err, domain) {
                if (err)
                    callback(err);
                else {
                    domainId = domain._id;

                    db.Cache.update({_id: {$in: caches}}, {$addToSet: {domains: domainId}}, {multi: true}, function (err) {
                        if (err)
                            callback(err);
                        else {
                            savedDomains.push(domain);

                            _updateCacheStore(cacheStoreChanges, callback);
                        }
                    });
                }
            });
        });
}

function _updateCacheStore(cacheStoreChanges, callback) {
    if (cacheStoreChanges && cacheStoreChanges.length > 0) {
        async.forEachOf(cacheStoreChanges, function (change, idx, callback) {
            db.Cache.update({_id: {$eq: change.cacheId}}, change.change, {}, function (err) {
                if (err)
                    callback(err);
                else
                    callback();
            });
        }, callback);
    }
    else
        callback();
}

function _save(domains, res) {
    var savedDomains = [];
    var generatedCaches = [];

    if (domains && domains.length > 0)
        async.forEachOf(domains, function(domain, idx, callback) {
            if (domain.newCache) {
                db.Cache.findOne({space: domain.space, name: domain.newCache.name}, function (err, cache) {
                    if (db.processed(err, res))
                        if (cache) {
                            // Cache already exists, just save domain model.
                            domain.caches = [cache._id];

                            _saveDomainModel(domain, savedDomains, callback);
                        }
                        else {
                            // If cache not found, then create it and associate with domain model.
                            var newCache = domain.newCache;
                            newCache.space = domain.space;

                            (new db.Cache(newCache)).save(function (err, cache) {
                                var cacheId = cache._id;

                                if (db.processed(err, res)) {
                                    db.Cluster.update({_id: {$in: cache.clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                        if (db.processed(err, res)) {
                                            domain.caches = [cacheId];
                                            generatedCaches.push(cache);

                                            _saveDomainModel(domain, savedDomains, callback);
                                        }
                                    });
                                }
                            });
                        }
                });
            }
            else
                _saveDomainModel(domain, savedDomains, callback);
        }, function (err) {
            if (err)
                res.status(500).send(err.message);
            else
                res.send({ savedDomains: savedDomains, generatedCaches: generatedCaches });
        });
    else
        res.status(500).send('Nothing to save!');
}

/**
 * Save domain model.
 */
router.post('/save', function (req, res) {
    _save([req.body], res);
});

/**
 * Batch save domain models.
 */
router.post('/save/batch', function (req, res) {
    _save(req.body, res);
});

/**
 * Remove domain model by ._id.
 */
router.post('/remove', function (req, res) {
    db.DomainModel.remove(req.body, function (err) {
        if (db.processed(err, res))
            res.sendStatus(200);
    })
});

/**
 * Remove all domain models.
 */
router.post('/remove/all', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            db.DomainModel.remove({space: {$in: space_ids}}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        }
    });
});

/**
 * Remove all generated demo domain models and caches.
 */
router.post('/remove/demo', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            // Remove all demo domain models.
            db.DomainModel.remove({$and: [{space: {$in: space_ids}}, {demo: true}]}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                // Remove all demo caches.
                db.Cache.remove({$and: [{space: {$in: space_ids}}, {demo: true}]}, function (err) {
                    if (err)
                        return res.status(500).send(err.message);

                    res.sendStatus(200);
                });
            });
        }
    });
});


module.exports = router;
