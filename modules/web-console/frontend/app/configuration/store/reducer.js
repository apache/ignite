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

import difference from 'lodash/difference';
import capitalize from 'lodash/capitalize';

export const LOAD_LIST = Symbol('LOAD_LIST');
export const ADD_CLUSTER = Symbol('ADD_CLUSTER');
export const ADD_CLUSTERS = Symbol('ADD_CLUSTERS');
export const REMOVE_CLUSTERS = Symbol('REMOVE_CLUSTERS');
export const UPDATE_CLUSTER = Symbol('UPDATE_CLUSTER');
export const UPSERT_CLUSTERS = Symbol('UPSERT_CLUSTERS');
export const ADD_CACHE = Symbol('ADD_CACHE');
export const UPDATE_CACHE = Symbol('UPDATE_CACHE');
export const UPSERT_CACHES = Symbol('UPSERT_CACHES');
export const REMOVE_CACHE = Symbol('REMOVE_CACHE');

import {
    REMOVE_CLUSTER_ITEMS_CONFIRMED
} from './actionTypes';

const defaults = {clusters: new Map(), caches: new Map(), spaces: new Map()};

const mapByID = (items) => {
    return Array.isArray(items) ? new Map(items.map((item) => [item._id, item])) : new Map(items);
};

export const reducer = (state = defaults, action) => {
    switch (action.type) {
        case LOAD_LIST: {
            return {
                clusters: mapByID(action.list.clusters),
                domains: mapByID(action.list.domains),
                caches: mapByID(action.list.caches),
                spaces: mapByID(action.list.spaces),
                plugins: mapByID(action.list.plugins)
            };
        }

        case ADD_CLUSTER: {
            return Object.assign({}, state, {
                clusters: new Map([...state.clusters.entries(), [action.cluster._id, action.cluster]])
            });
        }

        case ADD_CLUSTERS: {
            return Object.assign({}, state, {
                clusters: new Map([...state.clusters.entries(), ...action.clusters.map((c) => [c._id, c])])
            });
        }

        case REMOVE_CLUSTERS: {
            return Object.assign({}, state, {
                clusters: new Map([...state.clusters.entries()].filter(([id, value]) => !action.clusterIDs.includes(id)))
            });
        }

        case UPDATE_CLUSTER: {
            const id = action._id || action.cluster._id;
            return Object.assign({}, state, {
                // clusters: new Map(state.clusters).set(id, Object.assign({}, state.clusters.get(id), action.cluster))
                clusters: new Map(Array.from(state.clusters.entries()).map(([_id, cluster]) => {
                    return _id === id
                        ? [action.cluster._id || _id, Object.assign({}, cluster, action.cluster)]
                        : [_id, cluster];
                }))
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


export const RECEIVE_CLUSTER_EDIT = Symbol('RECEIVE_CLUSTER_EDIT');
export const RECEIVE_CACHE_EDIT = Symbol('RECEIVE_CACHE_EDIT');
export const RECEIVE_IGFSS_EDIT = Symbol('RECEIVE_IGFSS_EDIT');
export const RECEIVE_IGFS_EDIT = Symbol('RECEIVE_IGFS_EDIT');
export const RECEIVE_MODELS_EDIT = Symbol('RECEIVE_MODELS_EDIT');
export const RECEIVE_MODEL_EDIT = Symbol('RECEIVE_MODEL_EDIT');

export const editReducer = (state = {originalCluster: null}, action) => {
    switch (action.type) {
        case RECEIVE_CLUSTER_EDIT:
            return {
                ...state,
                originalCluster: action.cluster
            };

        case RECEIVE_CACHE_EDIT: {
            return {
                ...state,
                originalCache: action.cache
            };
        }

        case RECEIVE_IGFSS_EDIT:
            return {
                ...state,
                originalIGFSs: action.igfss
            };

        case RECEIVE_IGFS_EDIT: {
            return {
                ...state,
                originalIGFS: action.igfs
            };
        }

        case RECEIVE_MODELS_EDIT:
            return {
                ...state,
                originalModels: action.models
            };

        case RECEIVE_MODEL_EDIT: {
            return {
                ...state,
                originalModel: action.model
            };
        }

        default:
            return state;
    }
};

export const SHOW_CONFIG_LOADING = Symbol('SHOW_CONFIG_LOADING');
export const HIDE_CONFIG_LOADING = Symbol('HIDE_CONFIG_LOADING');
const loadingDefaults = {isLoading: false, loadingText: 'Loading...'};

export const loadingReducer = (state = loadingDefaults, action) => {
    switch (action.type) {
        case SHOW_CONFIG_LOADING:
            return {...state, isLoading: true, loadingText: action.loadingText};

        case HIDE_CONFIG_LOADING:
            return {...state, isLoading: false};

        default:
            return state;
    }
};

export const setStoreReducerFactory = (actionTypes) => (state = new Set(), action = {}) => {
    switch (action.type) {
        case actionTypes.SET:
            return new Set(action.items.map((i) => i._id));

        case actionTypes.RESET:
            return new Set();

        case actionTypes.UPSERT:
            return action.items.reduce((acc, item) => {acc.add(item._id); return acc;}, new Set(state));

        case actionTypes.REMOVE:
            return action.items.reduce((acc, item) => {acc.delete(item); return acc;}, new Set(state));

        default:
            return state;
    }
};

export const mapStoreReducerFactory = (actionTypes) => (state = new Map(), action = {}) => {
    switch (action.type) {
        case actionTypes.SET:
            return new Map(action.items.map((i) => [i._id, i]));

        case actionTypes.RESET:
            return new Map();

        case actionTypes.UPSERT:
            if (!action.items.length)
                return state;

            return action.items.reduce((acc, item) => {acc.set(item._id, item); return acc;}, new Map(state));

        case actionTypes.REMOVE:
            if (!action.ids.length)
                return state;

            return action.ids.reduce((acc, id) => {acc.delete(id); return acc;}, new Map(state));

        default:
            return state;
    }
};

export const mapCacheReducerFactory = (actionTypes) => {
    const mapStoreReducer = mapStoreReducerFactory(actionTypes);

    return (state = {value: mapStoreReducer(), pristine: true}, action) => {
        switch (action.type) {
            case actionTypes.SET:
            case actionTypes.REMOVE:
            case actionTypes.UPSERT:
                return {
                    value: mapStoreReducer(state.value, action),
                    pristine: false
                };

            case actionTypes.RESET:
                return {
                    value: mapStoreReducer(state.value, action),
                    pristine: true
                };

            default:
                return state;
        }
    };
};

export const basicCachesActionTypes = {
    SET: 'SET_BASIC_CACHES',
    RESET: 'RESET_BASIC_CACHES',
    LOAD: 'LOAD_BASIC_CACHES',
    UPSERT: 'UPSERT_BASIC_CACHES',
    REMOVE: 'REMOVE_BASIC_CACHES'
};

export const mapStoreActionTypesFactory = (NAME) => ({
    SET: `SET_${NAME}`,
    RESET: `RESET_${NAME}`,
    UPSERT: `UPSERT_${NAME}`,
    REMOVE: `REMOVE_${NAME}`
});

export const clustersActionTypes = mapStoreActionTypesFactory('CLUSTERS');
export const shortClustersActionTypes = mapStoreActionTypesFactory('SHORT_CLUSTERS');
export const cachesActionTypes = mapStoreActionTypesFactory('CACHES');
export const shortCachesActionTypes = mapStoreActionTypesFactory('SHORT_CACHES');
export const modelsActionTypes = mapStoreActionTypesFactory('MODELS');
export const shortModelsActionTypes = mapStoreActionTypesFactory('SHORT_MODELS');
export const igfssActionTypes = mapStoreActionTypesFactory('IGFSS');
export const shortIGFSsActionTypes = mapStoreActionTypesFactory('SHORT_IGFSS');

export const itemsEditReducerFactory = (actionTypes) => {
    const setStoreReducer = setStoreReducerFactory(actionTypes);
    const mapStoreReducer = mapStoreReducerFactory(actionTypes);

    return (state = {ids: setStoreReducer(), changedItems: mapStoreReducer()}, action) => {
        switch (action.type) {
            case actionTypes.SET:
                return action.state;

            case actionTypes.LOAD:
                return {
                    ...state,
                    ids: setStoreReducer(state.ids, {...action, type: actionTypes.UPSERT})
                };

            case actionTypes.RESET:
            case actionTypes.UPSERT:
                return {
                    ids: setStoreReducer(state.ids, action),
                    changedItems: mapStoreReducer(state.changedItems, action)
                };

            case actionTypes.REMOVE:
                return {
                    ids: setStoreReducer(state.ids, {type: action.type, items: action.ids}),
                    changedItems: mapStoreReducer(state.changedItems, action)
                };

            default:
                return state;
        }
    };
};

export const editReducer2 = (state = editReducer2.getDefaults(), action) => {
    switch (action.type) {
        case 'SET_EDIT':
            return action.state;

        case 'EDIT_CLUSTER': {
            return {
                ...state,
                changes: {
                    ...['caches', 'models', 'igfss'].reduce((a, t) => ({
                        ...a,
                        [t]: {
                            ids: action.cluster ? action.cluster[t] || [] : [],
                            changedItems: []
                        }
                    }), state.changes),
                    cluster: action.cluster
                }
            };
        }

        case 'RESET_EDIT_CHANGES': {
            return {
                ...state,
                changes: {
                    ...['caches', 'models', 'igfss'].reduce((a, t) => ({
                        ...a,
                        [t]: {
                            ids: state.changes.cluster ? state.changes.cluster[t] || [] : [],
                            changedItems: []
                        }
                    }), state.changes),
                    cluster: {...state.changes.cluster}
                }
            };
        }

        case 'UPSERT_CLUSTER': {
            return {
                ...state,
                changes: {
                    ...state.changes,
                    cluster: action.cluster
                }
            };
        }

        case 'UPSERT_CLUSTER_ITEM': {
            const {itemType, item} = action;
            return {
                ...state,
                changes: {
                    ...state.changes,
                    [itemType]: {
                        ids: state.changes[itemType].ids.filter((_id) => _id !== item._id).concat(item._id),
                        changedItems: state.changes[itemType].changedItems.filter(({_id}) => _id !== item._id).concat(item)
                    }
                }
            };
        }

        case REMOVE_CLUSTER_ITEMS_CONFIRMED: {
            const {itemType, itemIDs} = action;

            return {
                ...state,
                changes: {
                    ...state.changes,
                    [itemType]: {
                        ids: state.changes[itemType].ids.filter((_id) => !itemIDs.includes(_id)),
                        changedItems: state.changes[itemType].changedItems.filter(({_id}) => !itemIDs.includes(_id))
                    }
                }
            };
        }

        default: return state;
    }
};

editReducer2.getDefaults = () => ({
    changes: ['caches', 'models', 'igfss'].reduce((a, t) => ({...a, [t]: {ids: [], changedItems: []}}), {cluster: null})
});

export const refsReducer = (refs) => (state, action) => {
    switch (action.type) {
        case 'ADVANCED_SAVE_COMPLETE_CONFIGURATION': {
            const newCluster = action.changedItems.cluster;
            const oldCluster = state.clusters.get(newCluster._id) || {};
            const val = Object.keys(refs).reduce((state, ref) => {
                if (!state || !state[refs[ref].store].size)
                    return state;

                const addedSources = new Set(difference(newCluster[ref], oldCluster[ref] || []));
                const removedSources = new Set(difference(oldCluster[ref] || [], newCluster[ref]));
                const changedSources = new Map(action.changedItems[ref].map((m) => [m._id, m]));

                const targets = new Map();

                const maybeTarget = (id) => {
                    if (!targets.has(id))
                        targets.set(id, {[refs[ref].at]: {add: new Set(), remove: new Set()}});

                    return targets.get(id);
                };

                [...state[refs[ref].store].values()].forEach((target) => {
                    target[refs[ref].at]
                    .filter((sourceID) => removedSources.has(sourceID))
                    .forEach((sourceID) => maybeTarget(target._id)[refs[ref].at].remove.add(sourceID));
                });

                [...addedSources.values()].forEach((sourceID) => {
                    (changedSources.get(sourceID)[refs[ref].store] || []).forEach((targetID) => {
                        maybeTarget(targetID)[refs[ref].at].add.add(sourceID);
                    });
                });

                action.changedItems[ref].filter((s) => !addedSources.has(s._id)).forEach((source) => {
                    const newSource = source;
                    const oldSource = state[ref].get(source._id);
                    const addedTargets = difference(newSource[refs[ref].store], oldSource[refs[ref].store]);
                    const removedCaches = difference(oldSource[refs[ref].store], newSource[refs[ref].store]);
                    addedTargets.forEach((targetID) => {
                        maybeTarget(targetID)[refs[ref].at].add.add(source._id);
                    });
                    removedCaches.forEach((targetID) => {
                        maybeTarget(targetID)[refs[ref].at].remove.add(source._id);
                    });
                });
                const result = [...targets.entries()]
                    .filter(([targetID]) => state[refs[ref].store].has(targetID))
                    .map(([targetID, changes]) => {
                        const target = state[refs[ref].store].get(targetID);
                        return [
                            targetID,
                            {
                                ...target,
                                [refs[ref].at]: target[refs[ref].at]
                                    .filter((sourceID) => !changes[refs[ref].at].remove.has(sourceID))
                                    .concat([...changes[refs[ref].at].add.values()])
                            }
                        ];
                    });

                return result.length
                    ? {
                        ...state,
                        [refs[ref].store]: new Map([...state[refs[ref].store].entries()].concat(result))
                    }
                    : state;
            }, state);

            return val;
        }

        default:
            return state;
    }
};

export const shortObjectsReducer = (state, action) => {
    switch (action.type) {
        case REMOVE_CLUSTER_ITEMS_CONFIRMED: {
            const {itemType, itemIDs} = action;

            const target = 'short' + capitalize(itemType);

            const oldItems = state[target];

            const newItems = {
                value: itemIDs.reduce((acc, id) => {acc.delete(id); return acc;}, oldItems.value),
                pristine: oldItems.pristine
            };

            return {
                ...state,
                [target]: newItems
            };
        }

        default:
            return state;
    }
};
