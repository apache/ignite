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

const _ = require('lodash');
const assert = require('chai').assert;
const injector = require('../injector');

const testClusters = require('../data/clusters.json');
const testCaches = require('../data/caches.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let clusterService;
let cacheService;
let mongo;
let errors;
let db;

suite('ClusterServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/clusters'),
            injector('services/caches'),
            injector('mongo'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_clusterService, _cacheService, _mongo, _errors, _db]) => {
                mongo = _mongo;
                clusterService = _clusterService;
                cacheService = _cacheService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Get cluster', (done) => {
        const _id = testClusters[0]._id;

        clusterService.get(testClusters[0].space, false, _id)
            .then((cluster) => {
                assert.isNotNull(cluster);
                assert.equal(cluster._id, _id);
            })
            .then(done)
            .catch(done);
    });

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

    test('List of all clusters in space', (done) => {
        clusterService.shortList(testAccounts[0]._id, false)
            .then((clusters) => {
                assert.equal(clusters.length, 2);

                assert.equal(clusters[0].name, 'cluster-igfs');
                assert.isNotNull(clusters[0].discovery);
                assert.equal(clusters[0].cachesCount, 2);
                assert.equal(clusters[0].modelsCount, 2);
                assert.equal(clusters[0].igfsCount, 1);

                assert.equal(clusters[1].name, 'cluster-caches');
                assert.isNotNull(clusters[1].discovery);
                assert.equal(clusters[1].cachesCount, 5);
                assert.equal(clusters[1].modelsCount, 3);
                assert.equal(clusters[1].igfsCount, 0);
            })
            .then(done)
            .catch(done);
    });

    test('Create new cluster from basic', (done) => {
        const cluster = _.head(testClusters);
        const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));

        db.drop()
            .then(() => Promise.all([mongo.Account.create(testAccounts), mongo.Space.create(testSpaces)]))
            .then(() => clusterService.upsertBasic(testAccounts[0]._id, false, {cluster, caches}))
            .then((output) => {
                assert.isNotNull(output);

                assert.equal(output.rowsAffected, 1);
            })
            .then(() => clusterService.get(testAccounts[0]._id, false, cluster._id))
            .then((savedCluster) => {
                assert.isNotNull(savedCluster);

                assert.equal(savedCluster._id, cluster._id);
                assert.equal(savedCluster.name, cluster.name);
                assert.notStrictEqual(savedCluster.caches, cluster.caches);

                assert.notStrictEqual(savedCluster, cluster);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, caches[0]._id))
            .then((cb1) => {
                assert.isNotNull(cb1);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, caches[1]._id))
            .then((cb2) => {
                assert.isNotNull(cb2);
            })
            .then(done)
            .catch(done);
    });

    // test('Create new cluster without space', (done) => {
    //     const cluster = _.cloneDeep(_.head(testClusters));
    //     const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));
    //
    //     delete cluster.space;
    //
    //     db.drop()
    //         .then(() => Promise.all([mongo.Account.create(testAccounts), mongo.Space.create(testSpaces)]))
    //         .then(() => clusterService.upsertBasic(testAccounts[0]._id, false, {cluster, caches}))
    //         .then(() => done())
    //         .catch(done);
    // });

    test('Create new cluster with duplicated name', (done) => {
        const cluster = _.cloneDeep(_.head(testClusters));
        const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));

        cluster.name = _.last(testClusters).name;

        clusterService.upsertBasic(testAccounts[0]._id, false, {cluster, caches})
            .then(done)
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Update cluster from basic', (done) => {
        const cluster = _.cloneDeep(_.head(testClusters));

        cluster.communication.tcpNoDelay = false;
        cluster.igfss = [];

        cluster.memoryConfiguration = {
            defaultMemoryPolicySize: 10,
            memoryPolicies: [
                {
                    name: 'default',
                    maxSize: 100
                }
            ]
        };

        cluster.caches = _.dropRight(cluster.caches, 1);

        const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));

        _.head(caches).cacheMode = 'REPLICATED';
        _.head(caches).readThrough = false;

        clusterService.upsertBasic(testAccounts[0]._id, false, {cluster, caches})
            .then(() => clusterService.get(testAccounts[0]._id, false, cluster._id))
            .then((savedCluster) => {
                assert.isNotNull(savedCluster);

                assert.deepEqual(_.invokeMap(savedCluster.caches, 'toString'), cluster.caches);

                _.forEach(savedCluster.memoryConfiguration.memoryPolicies, (plc) => delete plc._id);

                assert.notExists(savedCluster.memoryConfiguration.defaultMemoryPolicySize);
                assert.deepEqual(savedCluster.memoryConfiguration.memoryPolicies, cluster.memoryConfiguration.memoryPolicies);

                assert.notDeepEqual(_.invokeMap(savedCluster.igfss, 'toString'), cluster.igfss);
                assert.notDeepEqual(savedCluster.communication, cluster.communication);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, _.head(caches)._id))
            .then((cb1) => {
                assert.isNotNull(cb1);
                assert.equal(cb1.cacheMode, 'REPLICATED');
                assert.isTrue(cb1.readThrough);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, _.head(testClusters).caches[1]))
            .then((c2) => {
                assert.isNull(c2);
            })
            .then(done)
            .catch(done);
    });

    test('Update cluster from basic with cache removing', (done) => {
        const cluster = _.cloneDeep(_.head(testClusters));

        const removedCache = _.head(cluster.caches);
        const upsertedCache = _.last(cluster.caches);

        _.pull(cluster.caches, removedCache);

        const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));

        db.drop()
            .then(() => Promise.all([mongo.Account.create(testAccounts), mongo.Space.create(testSpaces)]))
            .then(() => clusterService.upsertBasic(testAccounts[0]._id, false, {cluster, caches}))
            .then(() => cacheService.get(testAccounts[0]._id, false, removedCache))
            .then((cache) => {
                assert.isNull(cache);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, upsertedCache))
            .then((cache) => {
                assert.isNotNull(cache);

                done();
            })
            .catch(done);
    });

    test('Update cluster from advanced with cache removing', (done) => {
        const cluster = _.cloneDeep(_.head(testClusters));

        cluster.communication.tcpNoDelay = false;
        cluster.igfss = [];

        cluster.memoryConfiguration = {
            defaultMemoryPolicySize: 10,
            memoryPolicies: [
                {
                    name: 'default',
                    maxSize: 100
                }
            ]
        };

        const removedCache = _.head(cluster.caches);
        const upsertedCache = _.last(cluster.caches);

        _.pull(cluster.caches, removedCache);

        const caches = _.filter(testCaches, ({_id}) => _.includes(cluster.caches, _id));

        clusterService.upsert(testAccounts[0]._id, false, {cluster, caches})
            .then(() => clusterService.get(testAccounts[0]._id, false, cluster._id))
            .then((savedCluster) => {
                assert.isNotNull(savedCluster);

                assert.deepEqual(_.invokeMap(savedCluster.caches, 'toString'), cluster.caches);

                _.forEach(savedCluster.memoryConfiguration.memoryPolicies, (plc) => delete plc._id);

                assert.deepEqual(savedCluster.memoryConfiguration, cluster.memoryConfiguration);

                assert.deepEqual(_.invokeMap(savedCluster.igfss, 'toString'), cluster.igfss);
                assert.deepEqual(savedCluster.communication, cluster.communication);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, removedCache))
            .then((cache) => {
                assert.isNull(cache);
            })
            .then(() => cacheService.get(testAccounts[0]._id, false, upsertedCache))
            .then((cache) => {
                assert.isNotNull(cache);

                done();
            })
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
