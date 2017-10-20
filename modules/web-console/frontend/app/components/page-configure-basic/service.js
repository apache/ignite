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

import cloneDeep from 'lodash/cloneDeep';

import {
    SET_CLUSTER,
    ADD_NEW_CACHE,
    REMOVE_CACHE,
    SET_SELECTED_CACHES,
    isNewItem
} from './reducer';

const makeId = (() => {
    let id = -1;
    return () => id--;
})();

export default class PageConfigureBasic {
    static $inject = [
        '$q',
        'IgniteMessages',
        'Clusters',
        'Caches',
        'ConfigureState',
        'PageConfigure'
    ];

    constructor($q, messages, clusters, caches, ConfigureState, pageConfigure) {
        Object.assign(this, {$q, messages, clusters, caches, ConfigureState, pageConfigure});
    }

    saveClusterAndCaches(cluster, caches) {
        // TODO IGNITE-5476 Implement single backend API method with transactions and use that instead
        const stripFakeID = (item) => Object.assign({}, item, {_id: isNewItem(item) ? void 0 : item._id});
        const noFakeIDCaches = caches.map(stripFakeID);
        cluster = cloneDeep(stripFakeID(cluster));
        return this.$q.all(noFakeIDCaches.map((cache) => (
            this.caches.saveCache(cache)
                .then(
                    ({data}) => data,
                    (e) => {
                        this.messages.showError(e);
                        return this.$q.resolve(null);
                    }
                )
        )))
        .then((cacheIDs) => {
            // Make sure we don't loose new IDs even if some requests fail
            this.pageConfigure.upsertCaches(
                cacheIDs.map((_id, i) => {
                    if (!_id) return;
                    const cache = caches[i];
                    return Object.assign({}, cache, {
                        _id,
                        clusters: cluster._id ? [...cache.clusters, cluster._id] : cache.clusters
                    });
                }).filter((v) => v)
            );

            cluster.caches = cacheIDs.map((_id, i) => _id || noFakeIDCaches[i]._id).filter((v) => v);
            this.setSelectedCaches(cluster.caches);
            caches.forEach((cache, i) => {
                if (isNewItem(cache) && cacheIDs[i]) this.removeCache(cache);
            });
            return cacheIDs;
        })
        .then((cacheIDs) => {
            if (cacheIDs.indexOf(null) !== -1) return this.$q.reject([cluster._id, cacheIDs]);
            return this.clusters.saveCluster(cluster)
            .catch((e) => {
                this.messages.showError(e);
                return this.$q.reject(e);
            })
            .then(({data: clusterID}) => {
                this.messages.showInfo(`Cluster ${cluster.name} was saved.`);
                // cache.clusters has to be updated again since cluster._id might have not existed
                // after caches were saved

                this.pageConfigure.upsertCaches(
                    cacheIDs.map((_id, i) => {
                        if (!_id) return;
                        const cache = caches[i];
                        return Object.assign({}, cache, {
                            _id,
                            clusters: cache.clusters.indexOf(clusterID) !== -1 ? cache.clusters : cache.clusters.concat(clusterID)
                        });
                    }).filter((v) => v)
                );
                this.pageConfigure.upsertClusters([
                    Object.assign(cluster, {
                        _id: clusterID
                    })
                ]);
                this.setCluster(clusterID);
                return [clusterID, cacheIDs];
            });
        });
    }

    setCluster(_id) {
        this.ConfigureState.dispatchAction(
            isNewItem({_id})
                ? {type: SET_CLUSTER, _id, cluster: this.clusters.getBlankCluster()}
                : {type: SET_CLUSTER, _id}
        );
    }

    addCache() {
        this.ConfigureState.dispatchAction({type: ADD_NEW_CACHE, _id: makeId()});
    }

    removeCache(cache) {
        this.ConfigureState.dispatchAction({type: REMOVE_CACHE, cache});
    }

    setSelectedCaches(cacheIDs) {
        this.ConfigureState.dispatchAction({type: SET_SELECTED_CACHES, cacheIDs});
    }
}
