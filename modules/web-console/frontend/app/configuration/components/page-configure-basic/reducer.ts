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

import cloneDeep from 'lodash/cloneDeep';

export const ADD_NEW_CACHE = Symbol('ADD_NEW_CACHE');
export const REMOVE_CACHE = Symbol('REMOVE_CACHE');
export const SET_SELECTED_CACHES = Symbol('SET_SELECTED_CACHES');
export const SET_CLUSTER = Symbol('SET_CLUSTER');

import {uniqueName} from 'app/utils/uniqueName';

const defaults = {
    clusterID: -1,
    cluster: null,
    newClusterCaches: [],
    oldClusterCaches: []
};

const defaultSpace = (root) => [...root.list.spaces.keys()][0];
const existingCaches = (caches, cluster) => {
    return cluster.caches.map((id) => {
        return cloneDeep(caches.get(id));
    }).filter((v) => v);
};
export const isNewItem = (item) => item && item._id < 0;

export const reducer = (state = defaults, action, root) => {
    switch (action.type) {
        case SET_CLUSTER: {
            const cluster = !isNewItem(action)
                ? cloneDeep(root.list.clusters.get(action._id))
                : Object.assign({}, action.cluster, {
                    _id: -1,
                    space: defaultSpace(root),
                    name: uniqueName('New cluster', [...root.list.clusters.values()], ({name, i}) => `${name} (${i})`)
                });

            return Object.assign({}, state, {
                clusterID: cluster._id,
                cluster,
                newClusterCaches: [],
                oldClusterCaches: existingCaches(root.list.caches, cluster)
            });
        }

        case ADD_NEW_CACHE: {
            const cache = {
                _id: action._id,
                space: defaultSpace(root),
                name: uniqueName('New cache', [...root.list.caches.values(), ...state.newClusterCaches], ({name, i}) => `${name} (${i})`),
                cacheMode: 'PARTITIONED',
                atomicityMode: 'ATOMIC',
                readFromBackup: true,
                copyOnRead: true,
                clusters: [],
                domains: [],
                cacheStoreFactory: {CacheJdbcBlobStoreFactory: {connectVia: 'DataSource'}},
                memoryPolicyName: 'default'
            };

            return Object.assign({}, state, {
                newClusterCaches: [...state.newClusterCaches, cache]
            });
        }

        case REMOVE_CACHE: {
            const cache = action.cache;

            return Object.assign({}, state, {
                newClusterCaches: isNewItem(cache)
                    ? state.newClusterCaches.filter((c) => c._id !== cache._id)
                    : state.newClusterCaches,
                oldClusterCaches: isNewItem(cache)
                    ? state.oldClusterCaches
                    : state.oldClusterCaches.filter((c) => c._id !== cache._id)
            });
        }

        case SET_SELECTED_CACHES: {
            const value = Object.assign({}, state, {
                cluster: Object.assign({}, state.cluster, {
                    caches: [...action.cacheIDs.filter((id) => id)]
                })
            });

            value.oldClusterCaches = existingCaches(root.list.caches, value.cluster);

            return value;
        }

        default:
            return state;
    }
};
