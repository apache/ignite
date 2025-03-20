

import {suite, test} from 'mocha';
import {assert} from 'chai';

import {
    SET_CLUSTER,
    ADD_NEW_CACHE,
    REMOVE_CACHE,
    SET_SELECTED_CACHES,
    reducer
} from './reducer';

suite('page-configure-basic component reducer', () => {
    test('Default state', () => {
        assert.deepEqual(reducer(void 0, {}), {
            clusterID: -1,
            cluster: null,
            newClusterCaches: [],
            oldClusterCaches: []
        });
    });

    test('SET_CLUSTER action', () => {
        const root = {
            list: {
                clusters: new Map([[1, {name: 'New cluster', id: 1, caches: [1]}]]),
                caches: new Map([[1, {}]]),
                spaces: new Map([[0, {}]])
            }
        };

        const defaultCluster = {
            id: null,
            discovery: {
                kind: 'Multicast',
                Vm: {addresses: ['127.0.0.1:47500..47510']},
                Multicast: {addresses: ['127.0.0.1:47500..47510']},
                Jdbc: {initSchema: true},
                Cloud: {regions: [], zones: []}
            },
            space: null,
            name: null,            
            caches: []
        };

        assert.deepEqual(
            reducer(void 0, {type: SET_CLUSTER, id: -1, cluster: defaultCluster}, root),
            {
                clusterID: -1,
                cluster: Object.assign({}, defaultCluster, {
                    id: -1,
                    name: 'New cluster (1)',
                    space: 0
                }),
                newClusterCaches: [],
                oldClusterCaches: []
            },
            'inits new cluster if id is fake'
        );

        assert.deepEqual(
            reducer(void 0, {type: SET_CLUSTER, id: 1}, root),
            {
                clusterID: 1,
                cluster: root.list.clusters.get(1),
                newClusterCaches: [],
                oldClusterCaches: [root.list.caches.get(1)]
            },
            'inits new cluster if id is real'
        );
    });

    test('ADD_NEW_CACHE action', () => {
        const state = {
            clusterID: -1,
            cluster: {},
            newClusterCaches: [{name: 'New cache (1)'}],
            oldClusterCaches: []
        };

        const root = {
            list: {
                caches: new Map([[1, {name: 'New cache'}]]),
                spaces: new Map([[1, {}]])
            }
        };

        const defaultCache = {
            id: null,
            space: null,
            name: null,
            cacheMode: 'PARTITIONED',
            atomicityMode: 'ATOMIC',
            readFromBackup: true,
            copyOnRead: true,
            clusters: [],
            domains: [],
            cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}},
            memoryPolicyName: 'default'
        };

        assert.deepEqual(
            reducer(state, {type: ADD_NEW_CACHE, id: -1}, root),
            {
                clusterID: -1,
                cluster: {},
                newClusterCaches: [
                    {name: 'New cache (1)'},
                    Object.assign({}, defaultCache, {
                        id: -1,
                        space: 1,
                        name: 'New cache (2)'
                    })
                ],
                oldClusterCaches: []
            },
            'adds new cache'
        );
    });

    test('REMOVE_CACHE action', () => {
        const state = {
            newClusterCaches: [{id: -1}],
            oldClusterCaches: [{id: 1}]
        };

        assert.deepEqual(
            reducer(state, {type: REMOVE_CACHE, cache: {id: null}}),
            state,
            'removes nothing if there\'s no matching cache'
        );

        assert.deepEqual(
            reducer(state, {type: REMOVE_CACHE, cache: {id: -1}}),
            {
                newClusterCaches: [],
                oldClusterCaches: [{id: 1}]
            },
            'removes new cluster cache'
        );

        assert.deepEqual(
            reducer(state, {type: REMOVE_CACHE, cache: {id: 1}}),
            {
                newClusterCaches: [{id: -1}],
                oldClusterCaches: []
            },
            'removes old cluster cache'
        );
    });

    test('SET_SELECTED_CACHES action', () => {
        const state = {
            cluster: {caches: []},
            oldClusterCaches: []
        };

        const root = {
            list: {caches: new Map([[1, {id: 1}], [2, {id: 2}], [3, {id: 3}]])}
        };

        assert.deepEqual(
            reducer(state, {type: SET_SELECTED_CACHES, cacheIDs: []}, root),
            state,
            'select no caches if action.cacheIDs is empty'
        );

        assert.deepEqual(
            reducer(state, {type: SET_SELECTED_CACHES, cacheIDs: [1]}, root),
            {
                cluster: {caches: [1]},
                oldClusterCaches: [{id: 1}]
            },
            'selects existing cache'
        );

        assert.deepEqual(
            reducer(state, {type: SET_SELECTED_CACHES, cacheIDs: [1, 2, 3]}, root),
            {
                cluster: {caches: [1, 2, 3]},
                oldClusterCaches: [{id: 1}, {id: 2}, {id: 3}]
            },
            'selects three existing caches'
        );
    });
});
