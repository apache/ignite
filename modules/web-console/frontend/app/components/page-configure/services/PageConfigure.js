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

import {
    ADD_CLUSTER,
    UPDATE_CLUSTER,
    UPSERT_CLUSTERS,
    LOAD_LIST,
    UPSERT_CACHES
} from '../reducer';

export default class PageConfigure {
    static $inject = ['IgniteConfigurationResource', '$state', '$q', 'ConfigureState'];

    constructor(configuration, $state, $q, ConfigureState) {
        Object.assign(this, {configuration, $state, $q, ConfigureState});
    }

    onStateEnterRedirect(toState) {
        if (toState.name !== 'base.configuration.tabs')
            return this.$q.resolve();

        return this.configuration.read()
            .then((data) => {
                this.loadList(data);

                return this.$q.resolve(data.clusters.length
                    ? 'base.configuration.tabs.advanced'
                    : 'base.configuration.tabs.basic');
            });
    }

    loadList(list) {
        this.ConfigureState.dispatchAction({type: LOAD_LIST, list});
    }

    addCluster(cluster) {
        this.ConfigureState.dispatchAction({type: ADD_CLUSTER, cluster});
    }

    updateCluster(cluster) {
        this.ConfigureState.dispatchAction({type: UPDATE_CLUSTER, cluster});
    }

    upsertCaches(caches) {
        this.ConfigureState.dispatchAction({type: UPSERT_CACHES, caches});
    }

    upsertClusters(clusters) {
        this.ConfigureState.dispatchAction({type: UPSERT_CLUSTERS, clusters});
    }
}
