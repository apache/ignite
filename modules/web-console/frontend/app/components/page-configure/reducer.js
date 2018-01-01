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

export const LOAD_LIST = Symbol('LOAD_LIST');
export const ADD_CLUSTER = Symbol('ADD_CLUSTER');
export const REMOVE_CLUSTER = Symbol('REMOVE_CLUSTER');
export const UPDATE_CLUSTER = Symbol('UPDATE_CLUSTER');
export const UPSERT_CLUSTERS = Symbol('UPSERT_CLUSTERS');
export const ADD_CACHE = Symbol('ADD_CACHE');
export const UPDATE_CACHE = Symbol('UPDATE_CACHE');
export const UPSERT_CACHES = Symbol('UPSERT_CACHES');
export const REMOVE_CACHE = Symbol('REMOVE_CACHE');

const defaults = {clusters: new Map(), caches: new Map(), spaces: new Map()};
const mapByID = (array) => {
    return new Map(array.map((item) => [item._id, item]));
};

export const reducer = (state = defaults, action) => {
    switch (action.type) {
        case LOAD_LIST: {
            return {
                clusters: mapByID(action.list.clusters),
                caches: mapByID(action.list.caches),
                spaces: mapByID(action.list.spaces)
            };
        }
        case ADD_CLUSTER: {
            return Object.assign({}, state, {
                clusters: new Map([...state.clusters.entries(), [action.cluster._id, action.cluster]])
            });
        }
        case REMOVE_CLUSTER:
            return state;
        case UPDATE_CLUSTER: {
            const id = action.cluster._id;
            return Object.assign({}, state, {
                clusters: new Map(state.clusters).set(id, Object.assign({}, state.clusters.get(id), action.cluster))
            });
        }
        case UPSERT_CLUSTERS: {
            return action.clusters.reduce((state, cluster) => reducer(state, {
                type: state.clusters.has(cluster._id) ? UPDATE_CLUSTER : ADD_CLUSTER,
                cluster
            }), state);
        }
        case ADD_CACHE: {
            return Object.assign({}, state, {
                caches: new Map([...state.caches.entries(), [action.cache._id, action.cache]])
            });
        }
        case UPDATE_CACHE: {
            const id = action.cache._id;
            return Object.assign({}, state, {
                caches: new Map(state.caches).set(id, Object.assign({}, state.caches.get(id), action.cache))
            });
        }
        case UPSERT_CACHES: {
            return action.caches.reduce((state, cache) => reducer(state, {
                type: state.caches.has(cache._id) ? UPDATE_CACHE : ADD_CACHE,
                cache
            }), state);
        }
        case REMOVE_CACHE:
            return state;
        default:
            return state;
    }
};
