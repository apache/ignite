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

var router = require('express').Router();
var db = require('../db');

router.get('/rate', function (req, res) {
    res.render('sql/paragraph-rate', {});
});

router.get('/chart-settings', function (req, res) {
    res.render('sql/chart-settings', {});
});

router.get('/cache-metadata', function (req, res) {
    res.render('sql/cache-metadata', {});
});

router.get('/:noteId', function (req, res) {
    res.render('sql/sql', {noteId: req.params.noteId});
});

module.exports = router;
