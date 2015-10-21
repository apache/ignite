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

/* GET IGFS page. */
router.get('/', function (req, res) {
    res.render('configuration/igfs');
});

/**
 * Get spaces and IGFSs accessed for user account.
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
                    // Get all IGFSs for spaces.
                    db.Igfs.find({space: {$in: space_ids}}).sort('name').exec(function (err, igfss) {
                        if (db.processed(err, res)) {
                            _.forEach(igfss, function (igfs) {
                                // Remove deleted clusters.
                                igfs.clusters = _.filter(igfs.clusters, function (clusterId) {
                                    return _.findIndex(clusters, function (cluster) {
                                            return cluster._id.equals(clusterId);
                                        }) >= 0;
                                });
                            });

                            res.json({
                                spaces: spaces,
                                clusters: clusters.map(function (cluster) {
                                    return {value: cluster._id, label: cluster.name};
                                }),
                                igfss: igfss
                            });
                        }
                    });                }
            });
        }
    });
});

/**
 * Save IGFS.
 */
router.post('/save', function (req, res) {
    var params = req.body;
    var igfsId = params._id;
    var clusters = params.clusters;

    if (params._id) {
        db.Igfs.update({_id: igfsId}, params, {upsert: true}, function (err) {
            if (db.processed(err, res))
                db.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}, function (err) {
                    if (db.processed(err, res))
                        db.Cluster.update({_id: {$nin: clusters}}, {$pull: {igfss: igfsId}}, {multi: true}, function (err) {
                            if (db.processed(err, res))
                                res.send(params._id);
                        });
                });
        })
    }
    else
        db.Igfs.findOne({space: params.space, name: params.name}, function (err, igfs) {
            if (db.processed(err, res)) {
                if (igfs)
                    return res.status(500).send('IGFS with name: "' + igfs.name + '" already exist.');

                (new db.Igfs(params)).save(function (err, igfs) {
                    if (db.processed(err, res)) {
                        igfsId = igfs._id;

                        db.Cluster.update({_id: {$in: clusters}}, {$addToSet: {igfss: igfsId}}, {multi: true}, function (err) {
                            if (db.processed(err, res))
                                res.send(igfsId);
                        });
                    }
                });
            }
        });
});

/**
 * Remove IGFS by ._id.
 */
router.post('/remove', function (req, res) {
    db.Igfs.remove(req.body, function (err) {
        if (db.processed(err, res))
            res.sendStatus(200);
    })
});

/**
 * Remove all IGFSs.
 */
router.post('/remove/all', function (req, res) {
    var user_id = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: user_id}, {usedBy: {$elemMatch: {account: user_id}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var space_ids = spaces.map(function (value) {
                return value._id;
            });

            db.Igfs.remove({space: {$in: space_ids}}, function (err) {
                if (err)
                    return res.status(500).send(err.message);

                res.sendStatus(200);
            })
        }
    });
});

module.exports = router;
