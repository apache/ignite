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

import {suite, test} from 'mocha';
import {assert} from 'chai';

import {spy} from 'sinon';

import {
    SET_CLUSTER,
    SET_SELECTED_CACHES,
    REMOVE_CACHE
} from './reducer';
import Provider from './service';

const mocks = () => new Map([
    ['$q', Promise],
    ['messages', {
        showInfo: spy(),
        showError: spy()
    }],
    ['clusters', {
        _nextID: 1,
        saveCluster: spy(function(c) {
            if (this._nextID === 2) return Promise.reject(`Cluster with name ${c.name} already exists`);
            return Promise.resolve({data: this._nextID++});
        }),
        getBlankCluster: spy(() => ({name: 'Cluster'}))
    }],
    ['caches', {
        _nextID: 1,
        saveCache: spy(function(c) {
            if (this._nextID === 3) return Promise.reject(`Cache with name ${c.name} already exists`);
            return Promise.resolve({data: c._id || this._nextID++});
        })
    }],
    ['ConfigureState', {
        dispatchAction: spy()
    }],
    ['pageConfigure', {
        upsertCaches: spy(),
        upsertClusters: spy()
    }]
]);

suite('page-configure-basic service', () => {
    test('saveClusterAndCaches, new cluster only', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [];
        return service.saveClusterAndCaches(cluster, caches)
        .then(() => {
            assert.deepEqual(
                service.clusters.saveCluster.getCall(0).args[0],
                {_id: 1, name: 'New cluster', caches: []},
                'saves cluster'
            );
            assert.deepEqual(
                service.messages.showInfo.getCall(0).args,
                ['Cluster New cluster was saved.'],
                'shows correct message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.getCall(0).args[0],
                [{_id: 1, name: 'New cluster', caches: []}],
                'upserts cluster'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: []}],
                    [{type: SET_CLUSTER, _id: 1}]
                ],
                'sets current cluster'
            );
        });
    });
    test('saveClusterAndCaches, new cluster and new cache', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [{_id: -1, name: 'New cache', clusters: []}];
        return service.saveClusterAndCaches(cluster, caches)
        .then(() => {
            assert.deepEqual(
                service.clusters.saveCluster.getCall(0).args[0],
                {_id: 1, name: 'New cluster', caches: [1]},
                'saves cluster'
            );
            assert.deepEqual(
                service.caches.saveCache.getCall(0).args[0],
                {_id: void 0, name: 'New cache', clusters: []},
                'saves cache'
            );
            assert.deepEqual(
                service.messages.showInfo.getCall(0).args,
                ['Cluster New cluster was saved.'],
                'shows correct message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(0).args[0],
                [{_id: 1, clusters: [], name: 'New cache'}],
                'upserts cache without cluster id at first'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(1).args[0],
                [{_id: 1, clusters: [1], name: 'New cache'}],
                'upserts cache with cluster id afterwards'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.getCall(0).args[0],
                [{_id: 1, name: 'New cluster', caches: [1]}],
                'upserts the cluster'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: [1]}],
                    [{type: REMOVE_CACHE, cache: caches[0]}],
                    [{type: SET_CLUSTER, _id: 1}]
                ],
                'sets cache id and selects cluster'
            );
        });
    });
    test('saveClusterAndCaches, new cluster and two new caches', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [
            {_id: -1, name: 'New cache', clusters: []},
            {_id: -2, name: 'New cache (1)', clusters: []}
        ];
        return service.saveClusterAndCaches(cluster, caches)
        .then(() => {
            assert.deepEqual(
                service.messages.showInfo.getCall(0).args,
                ['Cluster New cluster was saved.'],
                'shows correct message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(0).args[0],
                [
                    {_id: 1, clusters: [], name: 'New cache'},
                    {_id: 2, clusters: [], name: 'New cache (1)'}
                ],
                'upserts all caches without cluster id at first'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(1).args[0],
                [
                    {_id: 1, clusters: [1], name: 'New cache'},
                    {_id: 2, clusters: [1], name: 'New cache (1)'}
                ],
                'upserts all caches with cluster id afterwards'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.getCall(0).args[0],
                [{_id: 1, name: 'New cluster', caches: [1, 2]}],
                'upserts the cluster with new cache IDs and cluster ID'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: [1, 2]}],
                    [{type: REMOVE_CACHE, cache: caches[0]}],
                    [{type: REMOVE_CACHE, cache: caches[1]}],
                    [{type: SET_CLUSTER, _id: 1}]
                ],
                'resets every cache and sets the cluster'
            );
        });
    });
    test('saveClusterAndCaches, new cluster with error', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [];
        service.clusters._nextID = 2;
        return service.saveClusterAndCaches(cluster, caches)
        .catch(() => {
            assert.deepEqual(
                service.messages.showError.getCall(0).args,
                ['Cluster with name New cluster already exists'],
                'shows correct error message'
            );
        });
    });
    test('saveClusterAndCaches, new cluster with error and one new cache', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [{_id: -1, name: 'New cache', clusters: []}];
        service.clusters._nextID = 2;
        return service.saveClusterAndCaches(cluster, caches)
        .catch(() => {
            assert.deepEqual(
                service.messages.showError.getCall(0).args,
                ['Cluster with name New cluster already exists'],
                'shows correct error message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(0).args[0],
                [{_id: 1, clusters: [], name: 'New cache'}],
                'upserts cache only once'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.args,
                [],
                'does not upsert cluster'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: [1]}],
                    [{type: REMOVE_CACHE, cache: caches[0]}]
                ],
                'dispatches only cache reset actions'
            );
        });
    });
    test('saveClusterAndCaches, new cluster with error, one new cache and one old cache', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: [3]};
        const caches = [
            {_id: -1, name: 'New cache', clusters: []},
            {_id: 3, name: 'Old cache', clusters: []}
        ];
        service.clusters._nextID = 2;
        return service.saveClusterAndCaches(cluster, caches)
        .catch(() => {
            assert.deepEqual(
                service.messages.showError.getCall(0).args,
                ['Cluster with name New cluster already exists'],
                'shows correct error message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(0).args[0],
                [
                    {_id: 1, clusters: [], name: 'New cache'},
                    {_id: 3, clusters: [], name: 'Old cache'}
                ],
                'upserts both caches once'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.args,
                [],
                'does not upsert cluster'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: [1, 3]}],
                    [{type: REMOVE_CACHE, cache: caches[0]}]
                ],
                'dispatches only cache reset actions'
            );
        });
    });
    test('saveClusterAndCaches, new cluster with error, new cache with error', () => {
        const service = new Provider(...mocks().values());
        const cluster = {_id: -1, name: 'New cluster', caches: []};
        const caches = [{_id: -1, name: 'New cache', clusters: []}];
        service.clusters._nextID = 2;
        service.caches._nextID = 3;
        return service.saveClusterAndCaches(cluster, caches)
        .catch(() => {
            assert.deepEqual(
                service.messages.showError.getCall(0).args,
                ['Cache with name New cache already exists'],
                'shows correct error message'
            );
            assert.deepEqual(
                service.pageConfigure.upsertCaches.getCall(0).args[0],
                [],
                'upserts no caches'
            );
            assert.deepEqual(
                service.pageConfigure.upsertClusters.args,
                [],
                'does not upsert cluster'
            );
            assert.deepEqual(
                service.ConfigureState.dispatchAction.args,
                [
                    [{type: SET_SELECTED_CACHES, cacheIDs: []}]
                ],
                'dispatches no actions'
            );
        });
    });
    suite('setCluster', () => {
        test('new cluster', () => {
            const service = new Provider(...mocks().values());
            service.setCluster(-1);
            assert.isOk(service.clusters.getBlankCluster.calledOnce, 'calls clusters.getBlankCluster');
            assert.deepEqual(
                service.ConfigureState.dispatchAction.lastCall.args[0],
                {type: SET_CLUSTER, _id: -1, cluster: service.clusters.getBlankCluster.returnValues[0]},
                'dispatches correct action'
            );
        });
        test('existing cluster', () => {
            const service = new Provider(...mocks().values());
            service.setCluster(1);
            assert.deepEqual(
                service.ConfigureState.dispatchAction.lastCall.args[0],
                {type: SET_CLUSTER, _id: 1},
                'dispatches correct action'
            );
        });
    });
});
