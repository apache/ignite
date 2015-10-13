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

var router = require('express').Router();
var db = require('../db');

/**
 * Get database presets.
 *
 * @param req Request.
 * @param res Response.
 */
router.post('/list', function (req, res) {
    var userId = req.currentUserId();

    // Get owned space and all accessed space.
    db.Space.find({$or: [{owner: userId}, {usedBy: {$elemMatch: {account: userId}}}]}, function (err, spaces) {
        if (db.processed(err, res)) {
            var spaceIds = spaces.map(function (value) {
                return value._id;
            });

            // Get all presets for spaces.
            db.DatabasePreset.find({space: {$in: spaceIds}}).exec(function (err, presets) {
                if (db.processed(err, res))
                    res.json({spaces: spaces, presets: presets});
            });
        }
    });
});

/**
 * Save database preset.
 */
router.post('/save', function (req, res) {
    var params = req.body;

    db.DatabasePreset.findOne({space: params.space, jdbcDriverJar: params.jdbcDriverJar}, function (err, preset) {
        if (db.processed(err, res)) {
            if (preset)
                db.DatabasePreset.update({space: params.space, jdbcDriverJar: params.jdbcDriverJar}, params, {upsert: true}, function (err) {
                    if (db.processed(err, res))
                        return res.sendStatus(200);
                });
            else
                (new db.DatabasePreset(params)).save(function (err) {
                    if (db.processed(err, res))
                        return res.sendStatus(200);
                });
        }
    });
});

module.exports = router;
