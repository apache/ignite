

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
                {clusters: new Map([[1, {id: 1}], [2, {id: 2}]])},
                {type: ADD_CLUSTER, cluster: {id: 3}}
            ),
            {
                clusters: new Map([[1, {id: 1}], [2, {id: 2}], [3, {id: 3}]])
            },
            'adds a cluster'
        );
    });

    test('REMOVE_CLUSTERS action', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {id: 1, name: 'Cluster 1'}], [2, {id: 2, name: 'Cluster 2'}]])},
                {type: REMOVE_CLUSTERS, clusterIDs: [1]}
            ),
            {clusters: new Map([[2, {id: 2, name: 'Cluster 2'}]])},
            'deletes clusters by id'
        );
    });

    test('UPDATE_CLUSTER action', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {id: 1, name: 'Hello'}]])},
                {type: UPDATE_CLUSTER, cluster: {id: 1, name: 'Hello world'}}
            ),
            {clusters: new Map([[1, {id: 1, name: 'Hello world'}]])},
            'updates a cluster'
        );
    });

    test('UPSERT_CLUSTERS', () => {
        assert.deepEqual(
            reducer(
                {clusters: new Map([
                    [1, {id: 1, name: 'One'}],
                    [2, {id: 2, name: 'Two'}]
                ])},
                {type: UPSERT_CLUSTERS, clusters: [{id: 1, name: '1', space: 1}]}
            ),
            {clusters: new Map([
                [1, {id: 1, name: '1', space: 1}],
                [2, {id: 2, name: 'Two'}]
            ])},
            'updates one cluster'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([
                    [1, {id: 1, name: 'One'}],
                    [2, {id: 2, name: 'Two'}]
                ])},
                {
                    type: UPSERT_CLUSTERS,
                    clusters: [
                        {id: 1, name: '1', space: 1},
                        {id: 2, name: '2'}
                    ]
                }
            ),
            {clusters: new Map([
                [1, {id: 1, name: '1', space: 1}],
                [2, {id: 2, name: '2'}]
            ])},
            'updates two clusters'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map()},
                {type: UPSERT_CLUSTERS, clusters: [{id: 1}]}
            ),
            {clusters: new Map([
                [1, {id: 1}]
            ])},
            'adds one cluster'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {id: 1}]])},
                {type: UPSERT_CLUSTERS, clusters: [{id: 2}, {id: 3}]}
            ),
            {clusters: new Map([
                [1, {id: 1}],
                [2, {id: 2}],
                [3, {id: 3}]
            ])},
            'adds two clusters'
        );

        assert.deepEqual(
            reducer(
                {clusters: new Map([[1, {id: 1}]])},
                {
                    type: UPSERT_CLUSTERS,
                    clusters: [
                        {id: 1, name: 'Test'},
                        {id: 2},
                        {id: 3}
                    ]
                }
            ),
            {clusters: new Map([
                [1, {id: 1, name: 'Test'}],
                [2, {id: 2}],
                [3, {id: 3}]
            ])},
            'adds and updates several clusters'
        );
    });

    test('ADD_CACHE action', () => {
        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {id: 1}], [2, {id: 2}]])},
                {type: ADD_CACHE, cache: {id: 3}}
            ),
            {
                caches: new Map([[1, {id: 1}], [2, {id: 2}], [3, {id: 3}]])
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
                {caches: new Map([[1, {id: 1, name: 'Hello'}]])},
                {type: UPDATE_CACHE, cache: {id: 1, name: 'Hello world'}}
            ),
            {caches: new Map([[1, {id: 1, name: 'Hello world'}]])},
            'updates a cache'
        );
    });

    test('UPSERT_CACHES', () => {
        assert.deepEqual(
            reducer(
                {caches: new Map([
                    [1, {id: 1, name: 'One'}],
                    [2, {id: 2, name: 'Two'}]
                ])},
                {type: UPSERT_CACHES, caches: [{id: 1, name: '1', space: 1}]}
            ),
            {caches: new Map([
                [1, {id: 1, name: '1', space: 1}],
                [2, {id: 2, name: 'Two'}]
            ])},
            'updates one cache'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([
                    [1, {id: 1, name: 'One'}],
                    [2, {id: 2, name: 'Two'}]
                ])},
                {
                    type: UPSERT_CACHES,
                    caches: [
                        {id: 1, name: '1', space: 1},
                        {id: 2, name: '2'}
                    ]
                }
            ),
            {caches: new Map([
                [1, {id: 1, name: '1', space: 1}],
                [2, {id: 2, name: '2'}]
            ])},
            'updates two caches'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map()},
                {type: UPSERT_CACHES, caches: [{id: 1}]}
            ),
            {caches: new Map([
                [1, {id: 1}]
            ])},
            'adds one cache'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {id: 1}]])},
                {type: UPSERT_CACHES, caches: [{id: 2}, {id: 3}]}
            ),
            {caches: new Map([
                [1, {id: 1}],
                [2, {id: 2}],
                [3, {id: 3}]
            ])},
            'adds two caches'
        );

        assert.deepEqual(
            reducer(
                {caches: new Map([[1, {id: 1}]])},
                {
                    type: UPSERT_CACHES,
                    caches: [
                        {id: 1, name: 'Test'},
                        {id: 2},
                        {id: 3}
                    ]
                }
            ),
            {caches: new Map([
                [1, {id: 1, name: 'Test'}],
                [2, {id: 2}],
                [3, {id: 3}]
            ])},
            'adds and updates several caches'
        );
    });
});
