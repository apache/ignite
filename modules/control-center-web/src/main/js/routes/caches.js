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

var _ = require('lodash');
var router = require('express').Router();
var db = require('../db');

/* GET caches page. */
router.get('/', function (req, res) {
    res.render('configuration/caches');
});

/**
 * Get spaces and caches accessed for user account.
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
                    // Get all caches type metadata for spaces.
                    db.CacheTypeMetadata.find({space: {$in: space_ids}}).sort('name').exec(function (err, metadatas) {
                        if (db.processed(err, res)) {
                            // Get all caches for spaces.
                            db.Cache.find({space: {$in: space_ids}}).sort('name').exec(function (err, caches) {
                                if (db.processed(err, res)) {
                                    _.forEach(caches, function (cache) {
                                        // Remove deleted clusters.
                                        cache.clusters = _.filter(cache.clusters, function (clusterId) {
                                            return _.findIndex(clusters, function (cluster) {
                                                    return cluster._id.equals(clusterId);
                                                }) >= 0;
                                        });

                                        // Remove deleted metadata.
                                        cache.metadatas = _.filter(cache.metadatas, function (metaId) {
                                            return _.findIndex(metadatas, function (meta) {
                                                    return meta._id.equals(metaId);
                                                }) >= 0;
                                        });
                                    });

                                    res.json({
                                        spaces: spaces,
                                        clusters: clusters.map(function (cluster) {
                                            return {value: cluster._id, label: cluster.name};
                                        }),
                                        metadatas: metadatas,
                                        caches: caches
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

/**
 * Save cache.
 */
router.post('/save', function (req, res) {
    var params = req.body;
    var cacheId = params._id;
    var clusters = params.clusters;
    var metadatas = params.metadatas;

    if (params._id) {
        db.Cache.update({_id: cacheId}, params, {upsert: true}, function (err) {
            if (db.processed(err, res))
                db.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                    if (db.processed(err, res))
                        db.Cluster.update({_id: {$nin: clusters}}, {$pull: {caches: cacheId}}, {multi: true}, function (err) {
                            if (db.processed(err, res))
                                db.CacheTypeMetadata.update({_id: {$in: metadatas}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                    if (db.processed(err, res))
                                        db.CacheTypeMetadata.update({_id: {$nin: metadatas}}, {$pull: {caches: cacheId}}, {multi: true}, function (err) {
                                            if (db.processed(err, res))
                                                res.send(params._id);
                                        });
                                });
                        });
                });
        })
    }
    else
        db.Cache.findOne({space: params.space, name: params.name}, function (err, cache) {
            if (db.processed(err, res)) {
                if (cache)
                    return res.status(500).send('Cache with name: "' + cache.name + '" already exist.');

                (new db.Cache(params)).save(function (err, cache) {
                    if (db.processed(err, res)) {
                        cacheId = cache._id;

                        db.Cluster.update({_id: {$in: clusters}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                            if (db.processed(err, res))
                                db.CacheTypeMetadata.update({_id: {$in: metadatas}}, {$addToSet: {caches: cacheId}}, {multi: true}, function (err) {
                                    if (db.processed(err, res))
                                        res.send(cacheId);
                                });
                        });
                    }
                });
            }
        });
});

/**
 * Remove cache by ._id.
 */
router.post('/remove', function (req, res) {
    db.Cache.remove(req.body, function (err) {
        if (db.processed(err, res))
            res.sendStatus(200);
    })
});

/**
 * Remove all caches.
 */
router.post('/remove/all', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            db.Cache.remove({space: {$in: space_ids}}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        }
    });
});

module.exports = router;
