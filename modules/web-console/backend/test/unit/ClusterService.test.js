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

let clusterService;
let mongo;
let errors;

suite('ClusterServiceTestsSuite', () => {
    const prepareUserSpaces = () => {
        return mongo.Account.create(testAccounts)
            .then((accounts) => {
                return Promise.all(accounts.map((account) => mongo.Space.create(
                    [
                        {name: 'Personal space', owner: account._id, demo: false},
                        {name: 'Demo space', owner: account._id, demo: true}
                    ]
                )))
                    .then((spaces) => [accounts, spaces]);
            });
    };

    suiteSetup(() => {
        return Promise.all([injector('services/clusters'),
            injector('mongo'),
            injector('errors')])
            .then(([_clusterService, _mongo, _errors]) => {
                mongo = _mongo;
                clusterService = _clusterService;
                errors = _errors;
            });
    });

    setup(() => {
        return Promise.all([
            mongo.Cluster.remove().exec(),
            mongo.Account.remove().exec(),
            mongo.Space.remove().exec()
        ]);
    });

    test('Create new cluster', (done) => {
        clusterService.merge(testClusters[0])
            .then((cluster) => {
                assert.isNotNull(cluster._id);

                return cluster._id;
            })
            .then((clusterId) => mongo.Cluster.findById(clusterId))
            .then((cluster) => {
                assert.isNotNull(cluster);
            })
            .then(done)
            .catch(done);
    });

    test('Update existed cluster', (done) => {
        const newName = 'NewUniqueName';

        clusterService.merge(testClusters[0])
            .then((cluster) => {
                const clusterBeforeMerge = Object.assign({}, testClusters[0], {_id: cluster._id, name: newName});

                return clusterService.merge(clusterBeforeMerge);
            })
            .then((cluster) => mongo.Cluster.findById(cluster._id))
            .then((clusterAfterMerge) => {
                assert.equal(clusterAfterMerge.name, newName);
            })
            .then(done)
            .catch(done);
    });

    test('Create duplicated cluster', (done) => {
        const dupleCluster = Object.assign({}, testClusters[0], {_id: null});

        clusterService.merge(testClusters[0])
            .then(() => clusterService.merge(dupleCluster))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed cluster', (done) => {
        clusterService.merge(testClusters[0])
            .then((existCluster) => {
                return mongo.Cluster.findById(existCluster._id)
                    .then((foundCluster) => clusterService.remove(foundCluster._id))
                    .then(({rowsAffected}) => {
                        assert.equal(rowsAffected, 1);
                    })
                    .then(() => mongo.Cluster.findById(existCluster._id))
                    .then((notFoundCluster) => {
                        assert.isNull(notFoundCluster);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Remove cluster without identifier', (done) => {
        clusterService.merge(testClusters[0])
            .then(() => clusterService.remove())
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed cluster', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        clusterService.merge(testClusters[0])
            .then(() => clusterService.remove(validNoExistingId))
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 0);
            })
            .then(done)
            .catch(done);
    });

    test('Remove all clusters in space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userCluster = Object.assign({}, testClusters[0], {space: spaces[0][0]._id});

                return clusterService.merge(userCluster)
                    .then(() => clusterService.removeAll(currentUser._id, false));
            })
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 1);
            })
            .then(done)
            .catch(done);
    });

    test('Get all clusters by space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const userCluster = Object.assign({}, testClusters[0], {space: spaces[0][0]._id});

                return clusterService.merge(userCluster)
                    .then((existCluster) => {
                        return clusterService.listBySpaces(spaces[0][0]._id)
                            .then((clusters) => {
                                assert.equal(clusters.length, 1);
                                assert.equal(clusters[0]._id.toString(), existCluster._id.toString());
                            });
                    });
            })
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
