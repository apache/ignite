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

// Fire me up!

module.exports = {
    implements: 'routes/domains',
    inject: ['require(lodash)', 'require(express)', 'mongo', 'services/domains']
};

module.exports.factory = (_, express, mongo, domainsService) => {
    return new Promise((factoryResolve) => {
        const router = new express.Router();

        /**
         * Save domain model.
         */
        router.post('/save', (req, res) => {
            const domain = req.body;

            domainsService.batchMerge([domain])
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Batch save domain models.
         */
        router.post('/save/batch', (req, res) => {
            const domains = req.body;

            domainsService.batchMerge(domains)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove domain model by ._id.
         */
        router.post('/remove', (req, res) => {
            const domainId = req.body._id;

            domainsService.remove(domainId)
                .then(res.api.ok)
                .catch(res.api.error);
        });

        /**
         * Remove all domain models.
         */
        router.post('/remove/all', (req, res) => {
            domainsService.removeAll(req.currentUserId(), req.header('IgniteDemoMode'))
                .then(res.api.ok)
                .catch(res.api.error);
        });

        factoryResolve(router);
    });
};

