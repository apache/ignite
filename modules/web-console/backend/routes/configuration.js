/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    implements: 'routes/configurations',
    inject: ['mongo', 'services/configurations']
};

module.exports.factory = function(mongo, configurationsService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Get all user configuration in current space.
         */
        router.get('/list', (req, res) => {
            configurationsService.list(req.currentUserId(), req.demo())
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Get user configuration in current space.
         */
        router.get('/:_id', (req, res) => {
            configurationsService.get(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};
