/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

var async = require('async');
var router = require('express').Router();
var db = require('../db');

/* GET metadata page. */
router.get('/', function (req, res) {
    res.render('configuration/metadata');
});

/* GET metadata load dialog. */
router.get('/metadata-load', function (req, res) {
    res.render('configuration/metadata-load');
});

/**
 * Get spaces and metadata accessed for user account.
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

            // Get all caches for spaces.
            db.Cache.find({space: {$in: space_ids}}).sort('name').exec(function (err, caches) {
                if (db.processed(err, res)) {
                    // Get all metadata for spaces.
                    db.CacheTypeMetadata.find({space: {$in: space_ids}}).sort('valueType').exec(function (err, metadatas) {
                        if (db.processed(err, res)) {
                            // Remove deleted caches.
                            _.forEach(metadatas, function (meta) {
                                meta.caches = _.filter(meta.caches, function (cacheId) {
                                    return _.findIndex(caches, function (cache) {
                                            return cache._id.equals(cacheId);
                                        }) >= 0;
                                });
                            });

                            res.json({
                                spaces: spaces,
                                caches: caches.map(function (cache) {
                                    return {value: cache._id, label: cache.name};
                                }),
                                metadatas: metadatas
                            });
                        }
                    });
                }
            });
        }
    });
});

function _save(metas, res) {
    var savedMetas = [];

    if (metas && metas.length > 0)
        async.forEachOf(metas, function(meta, idx, callback) {
            var metaId = meta._id;
            var caches = meta.caches;

            if (metaId)
                db.CacheTypeMetadata.update({_id: meta._id}, meta, {upsert: true}, function (err) {
                    if (err)
                        callback(err);
                    else
                        db.Cache.update({_id: {$in: caches}}, {$addToSet: {metadatas: metaId}}, {multi: true}, function (err) {
                            if (err)
                                callback(err);
                            else
                                db.Cache.update({_id: {$nin: caches}}, {$pull: {metadatas: metaId}}, {multi: true}, function (err) {
                                    if (err)
                                        callback(err);
                                    else {
                                        savedMetas.push(meta);

                                        callback();
                                    }
                                });
                        });
                });
            else {
                db.CacheTypeMetadata.findOne({space: meta.space, valueType: meta.valueType}, function (err, metadata) {
                    if (err)
                        callback(err);
                    else
                        if (metadata)
                            return callback('Cache type metadata with value type: "' + metadata.valueType + '" already exist.');

                        (new db.CacheTypeMetadata(meta)).save(function (err, metadata) {
                            if (err)
                                callback(err);
                            else {
                                metaId = metadata._id;

                                db.Cache.update({_id: {$in: caches}}, {$addToSet: {metadatas: metaId}}, {multi: true}, function (err) {
                                    if (err)
                                        callback(err);
                                    else {
                                        savedMetas.push(metadata);

                                        callback();
                                    }
                                });
                            }
                        });
                    });
                }
        }, function (err) {
            if (err)
                res.status(500).send(err);
            else
                res.send(savedMetas);
        });
    else
        res.status(500).send('Nothing to save!');
}

/**
 * Save metadata.
 */
router.post('/save', function (req, res) {
    _save([req.body], res);
});

/**
 * Batch save metadata .
 */
router.post('/save/batch', function (req, res) {
    _save(req.body, res);
});

/**
 * Remove metadata by ._id.
 */
router.post('/remove', function (req, res) {
    db.CacheTypeMetadata.remove(req.body, function (err) {
        if (db.processed(err, res))
            res.sendStatus(200);
    })
});

/**
 * Remove all metadata.
 */
router.post('/remove/all', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            db.CacheTypeMetadata.remove({space: {$in: space_ids}}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        }
    });
});

module.exports = router;
