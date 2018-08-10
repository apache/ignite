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

const _ = require('lodash');

const testAccounts = require('../data/accounts.json');
const testClusters = require('../data/clusters.json');
const testCaches = require('../data/caches.json');
const testDomains = require('../data/domains.json');
const testIgfss = require('../data/igfss.json');
const testSpaces = require('../data/spaces.json');

module.exports = {
    implements: 'dbHelper',
    inject: ['mongo', 'mongoose']
};

module.exports.factory = (mongo, mongoose) => {
    const prepareUserSpaces = () => Promise.all([mongo.Account.create(testAccounts), mongo.Space.create(testSpaces)]);
    const prepareClusters = () => mongo.Cluster.create(testClusters);
    const prepareDomains = () => mongo.DomainModel.create(testDomains);
    const prepareCaches = () => mongo.Cache.create(testCaches);
    const prepareIgfss = () => mongo.Igfs.create(testIgfss);

    const drop = () => {
        return Promise.all(_.map(mongoose.connection.collections, (collection) => collection.remove()));
    };

    const init = () => {
        return drop()
            .then(prepareUserSpaces)
            .then(prepareClusters)
            .then(prepareDomains)
            .then(prepareCaches)
            .then(prepareIgfss);
    };

    return {
        drop,
        init,
        mocks: {
            accounts: testAccounts,
            clusters: testClusters,
            caches: testCaches,
            domains: testDomains,
            igfss: testIgfss,
            spaces: testSpaces
        }
    };
};
