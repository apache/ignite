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
    implements: 'routes/clusters',
    inject: ['mongo', 'services/clusters', 'services/caches', 'services/domains', 'services/igfss']
};

module.exports.factory = function(mongo, clustersService, cachesService, domainsService, igfssService) {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        router.get('/:_id/caches', (req, res) => {
            cachesService.shortList(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.get('/:_id/models', (req, res) => {
            domainsService.shortList(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.get('/:_id/igfss', (req, res) => {
            igfssService.shortList(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.get('/:_id', (req, res) => {
            clustersService.get(req.currentUserId(), req.demo(), req.params._id)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.get('/', (req, res) => {
            clustersService.shortList(req.currentUserId(), req.demo())
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.put('/basic', (req, res) => {
            clustersService.upsertBasic(req.currentUserId(), req.demo(), req.body)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        router.put('/', (req, res) => {
            clustersService.upsert(req.currentUserId(), req.demo(), req.body)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Save cluster.
         */
        router.post('/save', (req, res) => {
            const cluster = req.body;

            clustersService.merge(cluster)
                .then((savedCluster) => res.api.ok(savedCluster._id))
                .catch(res.api.error);
        });

        /**
         * Remove cluster by ._id.
         */
        router.post('/remove', (req, res) => {
            const clusterId = req.body._id;

            clustersService.remove(clusterId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove all clusters.
         */
        router.post('/remove/all', (req, res) => {
            clustersService.removeAll(req.currentUserId(), req.header('IgniteDemoMode'))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};
