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

/* GET clusters page. */
router.get('/', function (req, res) {
    res.render('configuration/clusters');
});

/**
 * Get spaces and clusters accessed for user account.
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
            db.Cache.find({space: {$in: space_ids}}).sort('name').deepPopulate('metadatas').exec(function (err, caches) {
                if (db.processed(err, res)) {
                    // Get all clusters for spaces.
                    db.Cluster.find({space: {$in: space_ids}}).sort('name').exec(function (err, clusters) {
                        if (db.processed(err, res)) {
                            // Remove deleted caches.
                            _.forEach(clusters, function (cluster) {
                                cluster.caches = _.filter(cluster.caches, function (cacheId) {
                                    return _.findIndex(caches, function (cache) {
                                            return cache._id.equals(cacheId);
                                        }) >= 0;
                                });
                            });

                            res.json({spaces: spaces, caches: caches, clusters: clusters});
                        }
                    });
                }
            });
        }
    });
});

/**
 * Save cluster.
 */
router.post('/save', function (req, res) {
    var params = req.body;
    var clusterId = params._id;
    var caches = params.caches;

    if (params._id)
        db.Cluster.update({_id: params._id}, params, {upsert: true}, function (err) {
            if (db.processed(err, res))
                db.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, function(err) {
                    if (db.processed(err, res)) {
                        db.Cache.update({_id: {$nin: caches}}, {$pull: {clusters: clusterId}}, {multi: true}, function(err) {
                            if (db.processed(err, res))
                                res.send(params._id);
                        });
                    }
                });
        });
    else {
        db.Cluster.findOne({space: params.space, name: params.name}, function (err, cluster) {
            if (db.processed(err, res)) {
                if (cluster)
                    return res.status(500).send('Cluster with name: "' + cluster.name + '" already exist.');

                (new db.Cluster(params)).save(function (err, cluster) {
                    if (db.processed(err, res)) {
                        clusterId = cluster._id;

                        db.Cache.update({_id: {$in: caches}}, {$addToSet: {clusters: clusterId}}, {multi: true}, function (err) {
                            if (db.processed(err, res))
                                res.send(clusterId);
                        });
                    }
                });
            }
        });
    }
});

/**
 * Remove cluster by ._id.
 */
router.post('/remove', function (req, res) {
    db.Cluster.remove(req.body, function (err) {
        if (err)
            return res.status(500).send(err.message);

        res.sendStatus(200);
    })
});

/**
 * Remove all clusters.
 */
router.post('/remove/all', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            db.Cluster.remove({space: {$in: space_ids}}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        }
    });
});

module.exports = router;
