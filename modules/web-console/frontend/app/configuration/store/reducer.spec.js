/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import {suite, test} from 'mocha';
import {assert} from 'chai';

import {
    ADD_CLUSTER,
    REMOVE_CLUSTERS,
    UPDATE_CLUSTER,
    UPSERT_CLUSTERS,
    ADD_CACHE,
    UPDATE_CACHE,
    UPSERT_CACHES,
    REMOVE_CACHE,
    reducer
} from './reducer';

suite('page-configure component reducer', () => {
    test('Default state', () => {
        assert.deepEqual(
            reducer(void 0, {}),
            {
                clusters: new Map(),
                caches: new Map(),
                spaces: new Map()
            }
        );
    });

    test('ADD_CLUSTER action', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {_id: 1}], [2, {_id: 2}]])},
                {type: ADD_CLUSTER, cluster: {_id: 3}}
            ),
            {
                clusters: new Map([[1, {_id: 1}], [2, {_id: 2}], [3, {_id: 3}]])
            },
            'adds a cluster'
        );
    });

    test('REMOVE_CLUSTERS action', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {_id: 1, name: 'Cluster 1'}], [2, {_id: 2, name: 'Cluster 2'}]])},
                {type: REMOVE_CLUSTERS, clusterIDs: [1]}
            ),
            {clusters: new Map([[2, {_id: 2, name: 'Cluster 2'}]])},
            'deletes clusters by id'
        );
    });

    test('UPDATE_CLUSTER action', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {_id: 1, name: 'Hello'}]])},
                {type: UPDATE_CLUSTER, cluster: {_id: 1, name: 'Hello world'}}
            ),
            {clusters: new Map([[1, {_id: 1, name: 'Hello world'}]])},
            'updates a cluster'
        );
    });

    test('UPSERT_CLUSTERS', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([
                    [1, {_id: 1, name: 'One'}],
                    [2, {_id: 2, name: 'Two'}]
                ])},
                {type: UPSERT_CLUSTERS, clusters: [{_id: 1, name: '1', space: 1}]}
            ),
            {clusters: new Map([
                [1, {_id: 1, name: '1', space: 1}],
                [2, {_id: 2, name: 'Two'}]
            ])},
            'updates one cluster'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([
                    [1, {_id: 1, name: 'One'}],
                    [2, {_id: 2, name: 'Two'}]
                ])},
                {
                    type: UPSERT_CLUSTERS,
                    clusters: [
                        {_id: 1, name: '1', space: 1},
                        {_id: 2, name: '2'}
                    ]
                }
            ),
            {clusters: new Map([
                [1, {_id: 1, name: '1', space: 1}],
                [2, {_id: 2, name: '2'}]
            ])},
            'updates two clusters'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map()},
                {type: UPSERT_CLUSTERS, clusters: [{_id: 1}]}
            ),
            {clusters: new Map([
                [1, {_id: 1}]
            ])},
            'adds one cluster'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {_id: 1}]])},
                {type: UPSERT_CLUSTERS, clusters: [{_id: 2}, {_id: 3}]}
            ),
            {clusters: new Map([
                [1, {_id: 1}],
                [2, {_id: 2}],
                [3, {_id: 3}]
            ])},
            'adds two clusters'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {_id: 1}]])},
                {
                    type: UPSERT_CLUSTERS,
                    clusters: [
                        {_id: 1, name: 'Test'},
                        {_id: 2},
                        {_id: 3}
                    ]
                }
            ),
            {clusters: new Map([
                [1, {_id: 1, name: 'Test'}],
                [2, {_id: 2}],
                [3, {_id: 3}]
            ])},
            'adds and updates several clusters'
        );
    });

    test('ADD_CACHE action', () => {
        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {_id: 1}], [2, {_id: 2}]])},
                {type: ADD_CACHE, cache: {_id: 3}}
            ),
            {
                caches: new Map([[1, {_id: 1}], [2, {_id: 2}], [3, {_id: 3}]])
            },
            'adds a cache'
        );
    });

    test('REMOVE_CACHE action', () => {
        assert.deepEqual(
            reducer({}, {type: REMOVE_CACHE}),
            {},
            'does nothing yet'
        );
    });

    test('UPDATE_CACHE action', () => {
        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {_id: 1, name: 'Hello'}]])},
                {type: UPDATE_CACHE, cache: {_id: 1, name: 'Hello world'}}
            ),
            {caches: new Map([[1, {_id: 1, name: 'Hello world'}]])},
            'updates a cache'
        );
    });

    test('UPSERT_CACHES', () => {
        assert.deepEqual(
            reducer(
                {caches: new Map([
                    [1, {_id: 1, name: 'One'}],
                    [2, {_id: 2, name: 'Two'}]
                ])},
                {type: UPSERT_CACHES, caches: [{_id: 1, name: '1', space: 1}]}
            ),
            {caches: new Map([
                [1, {_id: 1, name: '1', space: 1}],
                [2, {_id: 2, name: 'Two'}]
            ])},
            'updates one cache'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([
                    [1, {_id: 1, name: 'One'}],
                    [2, {_id: 2, name: 'Two'}]
                ])},
                {
                    type: UPSERT_CACHES,
                    caches: [
                        {_id: 1, name: '1', space: 1},
                        {_id: 2, name: '2'}
                    ]
                }
            ),
            {caches: new Map([
                [1, {_id: 1, name: '1', space: 1}],
                [2, {_id: 2, name: '2'}]
            ])},
            'updates two caches'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map()},
                {type: UPSERT_CACHES, caches: [{_id: 1}]}
            ),
            {caches: new Map([
                [1, {_id: 1}]
            ])},
            'adds one cache'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {_id: 1}]])},
                {type: UPSERT_CACHES, caches: [{_id: 2}, {_id: 3}]}
            ),
            {caches: new Map([
                [1, {_id: 1}],
                [2, {_id: 2}],
                [3, {_id: 3}]
            ])},
            'adds two caches'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {_id: 1}]])},
                {
                    type: UPSERT_CACHES,
                    caches: [
                        {_id: 1, name: 'Test'},
                        {_id: 2},
                        {_id: 3}
                    ]
                }
            ),
            {caches: new Map([
                [1, {_id: 1, name: 'Test'}],
                [2, {_id: 2}],
                [3, {_id: 3}]
            ])},
            'adds and updates several caches'
        );
    });
});
