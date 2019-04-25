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
    implements: 'routes/igfss',
    inject: ['mongo', 'services/igfss']
};

module.exports.factory = function(mongo, igfssService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        router.get('/:_id', (req, res) => {
            igfssService.get(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.delete('/', (req, res) => {
            igfssService.remove(req.body.ids)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Save IGFS.
         */
        router.post('/save', (req, res) => {
            const igfs = req.body;

            igfssService.merge(igfs)
                .then((savedIgfs) => res.api.ok(savedIgfs._id))
                .catch(res.api.error);
        });

        /**
         * Remove IGFS by ._id.
         */
        router.post('/remove', (req, res) => {
            const igfsId = req.body._id;

            igfssService.remove(igfsId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove all IGFSs.
         */
        router.post('/remove/all', (req, res) => {
            igfssService.removeAll(req.currentUserId(), req.header('IgniteDemoMode'))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};

