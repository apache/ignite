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

const express = require('express');

// Fire me up!

module.exports = {
    implements: 'routes/caches',
    inject: ['mongo', 'services/caches']
};

module.exports.factory = function(mongo, cachesService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        router.get('/:_id', (req, res) => {
            cachesService.get(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.delete('/', (req, res) => {
            cachesService.remove(req.body.ids)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Save cache.
         */
        router.post('/save', (req, res) => {
            const cache = req.body;

            cachesService.merge(cache)
                .then((savedCache) => res.api.ok(savedCache._id))
                .catch(res.api.error);
        });

        /**
         * Remove cache by ._id.
         */
        router.post('/remove', (req, res) => {
            const cacheId = req.body._id;

            cachesService.remove(cacheId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove all caches.
         */
        router.post('/remove/all', (req, res) => {
            cachesService.removeAll(req.currentUserId(), req.header('IgniteDemoMode'))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};

