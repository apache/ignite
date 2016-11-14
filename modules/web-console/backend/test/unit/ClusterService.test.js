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

const assert = require('chai').assert;
const injector = require('../injector');
const testClusters = require('../data/clusters.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let clusterService;
let mongo;
let errors;
let db;

suite('ClusterServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/clusters'),
            injector('mongo'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_clusterService, _mongo, _errors, _db]) => {
                mongo = _mongo;
                clusterService = _clusterService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Create new cluster', (done) => {
        const dupleCluster = Object.assign({}, testClusters[0], {name: 'Other name'});

        delete dupleCluster._id;

        clusterService.merge(dupleCluster)
            .then((cluster) => mongo.Cluster.findById(cluster._id))
            .then((cluster) => assert.isNotNull(cluster))
            .then(done)
            .catch(done);
    });

    test('Update existed cluster', (done) => {
        const newName = 'NewUniqueName';

        const clusterBeforeMerge = Object.assign({}, testClusters[0], {name: newName});

        clusterService.merge(clusterBeforeMerge)
            .then((cluster) => mongo.Cluster.findById(cluster._id))
            .then((clusterAfterMerge) => assert.equal(clusterAfterMerge.name, newName))
            .then(done)
            .catch(done);
    });

    test('Create duplicated cluster', (done) => {
        const dupleCluster = Object.assign({}, testClusters[0]);

        delete dupleCluster._id;

        clusterService.merge(dupleCluster)
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed cluster', (done) => {
        clusterService.remove(testClusters[0]._id)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 1)
            )
            .then(() => mongo.Cluster.findById(testClusters[0]._id))
            .then((notFoundCluster) =>
                assert.isNull(notFoundCluster)
            )
            .then(done)
            .catch(done);
    });

    test('Remove cluster without identifier', (done) => {
        clusterService.remove()
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed cluster', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        clusterService.remove(validNoExistingId)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 0)
            )
            .then(done)
            .catch(done);
    });

    test('Get all clusters by space', (done) => {
        clusterService.listBySpaces(testSpaces[0]._id)
            .then((clusters) =>
                assert.equal(clusters.length, 2)
            )
            .then(done)
            .catch(done);
    });

    test('Remove all clusters in space', (done) => {
        clusterService.removeAll(testAccounts[0]._id, false)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 2)
            )
            .then(done)
            .catch(done);
    });

    test('Update linked entities on update cluster', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove cluster', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove all clusters in space', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });
});
