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

import {uniqueName} from 'app/utils/uniqueName';
import {of} from 'rxjs/observable/of';
import {empty} from 'rxjs/observable/empty';
import {combineLatest} from 'rxjs/observable/combineLatest';
import 'rxjs/add/operator/filter';
import 'rxjs/add/operator/mergeMap';
import 'rxjs/add/observable/combineLatest';
import {Observable} from 'rxjs/Observable';
import {defaultNames} from '../defaultNames';

import {default as Caches} from 'app/services/Caches';
import {default as Clusters} from 'app/services/Clusters';
import {default as IGFSs} from 'app/services/IGFSs';
import {default as Models} from 'app/services/Models';

const isDefined = (s) => s.filter((v) => v);

const selectItems = (path) => (s) => s.filter((s) => s).pluck(path).filter((v) => v);

const selectValues = (s) => s.map((v) => v && [...v.value.values()]);

export const selectMapItem = (mapPath, key) => (s) => s.pluck(mapPath).map((v) => v && v.get(key));

const selectMapItems = (mapPath, keys) => (s) => s.pluck(mapPath).map((v) => v && keys.map((key) => v.get(key)));

const selectItemToEdit = ({items, itemFactory, defaultName = '', itemID}) => (s) => s.switchMap((item) => {
    if (item)
        return of(Object.assign(itemFactory(), item));

    if (itemID === 'new')
        return items.take(1).map((items) => Object.assign(itemFactory(), {name: uniqueName(defaultName, items)}));

    if (!itemID)
        return of(null);

    return empty();
});

const currentShortItems = ({changesKey, shortKey}) => (state$) => {
    return Observable.combineLatest(
        state$.pluck('edit', 'changes', changesKey).let(isDefined).distinctUntilChanged(),
        state$.pluck(shortKey, 'value').let(isDefined).distinctUntilChanged()
    )
        .map(([{ids = [], changedItems}, shortItems]) => {
            if (!ids.length || !shortItems)
                return [];

            return ids.map((id) => changedItems.find(({_id}) => _id === id) || shortItems.get(id));
        })
        .map((v) => v.filter((v) => v));
};

const selectNames = (itemIDs, nameAt = 'name') => (items) => items
    .pluck('value')
    .map((items) => itemIDs.map((id) => items.get(id)[nameAt]));

export default class ConfigSelectors {
    static $inject = ['Caches', 'Clusters', 'IGFSs', 'Models'];

    /**
     * @param {Caches} Caches
     * @param {Clusters} Clusters
     * @param {IGFSs} IGFSs
     * @param {Models} Models
     */
    constructor(Caches, Clusters, IGFSs, Models) {
        this.Caches = Caches;
        this.Clusters = Clusters;
        this.IGFSs = IGFSs;
        this.Models = Models;

        /**
         * @param {string} id
         * @returns {(state$: Observable) => Observable<ig.config.model.DomainModel>}
         */
        this.selectModel = (id) => selectMapItem('models', id);
        /**
         * @returns {(state$: Observable) => Observable<{pristine: boolean, value: Map<string, ig.config.model.ShortDomainModel>}>}
         */
        this.selectShortModels = () => selectItems('shortModels');
        this.selectShortModelsValue = () => (state$) => state$.let(this.selectShortModels()).let(selectValues);
        /**
         * @returns {(state$: Observable) => Observable<Array<ig.config.cluster.ShortCluster>>}
         */
        this.selectShortClustersValue = () => (state$) => state$.let(this.selectShortClusters()).let(selectValues);
        /**
         * @returns {(state$: Observable) => Observable<Array<string>>}
         */
        this.selectClusterNames = (clusterIDs) => (state$) => state$
            .let(this.selectShortClusters())
            .let(selectNames(clusterIDs));
    }

    selectCluster = (id) => selectMapItem('clusters', id);

    selectShortClusters = () => selectItems('shortClusters');

    selectCache = (id) => selectMapItem('caches', id);

    selectIGFS = (id) => selectMapItem('igfss', id);

    selectShortCaches = () => selectItems('shortCaches');

    selectShortCachesValue = () => (state$) => state$.let(this.selectShortCaches()).let(selectValues);

    selectShortIGFSs = () => selectItems('shortIgfss');

    selectShortIGFSsValue = () => (state$) => state$.let(this.selectShortIGFSs()).let(selectValues);

    selectShortModelsValue = () => (state$) => state$.let(this.selectShortModels()).let(selectValues);

    selectCacheToEdit = (cacheID) => (state$) => state$
        .let(this.selectCache(cacheID))
        .distinctUntilChanged()
        .let(selectItemToEdit({
            items: state$.let(this.selectCurrentShortCaches),
            itemFactory: () => this.Caches.getBlankCache(),
            defaultName: defaultNames.cache,
            itemID: cacheID
        }));

    selectIGFSToEdit = (itemID) => (state$) => state$
        .let(this.selectIGFS(itemID))
        .distinctUntilChanged()
        .let(selectItemToEdit({
            items: state$.let(this.selectCurrentShortIGFSs),
            itemFactory: () => this.IGFSs.getBlankIGFS(),
            defaultName: defaultNames.igfs,
            itemID
        }));

    selectModelToEdit = (itemID) => (state$) => state$
        .let(this.selectModel(itemID))
        .distinctUntilChanged()
        .let(selectItemToEdit({
            items: state$.let(this.selectCurrentShortModels),
            itemFactory: () => this.Models.getBlankModel(),
            itemID
        }));

    selectClusterToEdit = (clusterID, defaultName = defaultNames.cluster) => (state$) => state$
        .let(this.selectCluster(clusterID))
        .distinctUntilChanged()
        .let(selectItemToEdit({
            items: state$.let(this.selectShortClustersValue()),
            itemFactory: () => this.Clusters.getBlankCluster(),
            defaultName,
            itemID: clusterID
        }));

    selectCurrentShortCaches = currentShortItems({changesKey: 'caches', shortKey: 'shortCaches'});

    selectCurrentShortIGFSs = currentShortItems({changesKey: 'igfss', shortKey: 'shortIgfss'});

    selectCurrentShortModels = currentShortItems({changesKey: 'models', shortKey: 'shortModels'});

    selectClusterShortCaches = (clusterID) => (state$) => {
        if (clusterID === 'new')
            return of([]);

        return combineLatest(
            state$.let(this.selectCluster(clusterID)).pluck('caches'),
            state$.let(this.selectShortCaches()).pluck('value'),
            (ids, items) => ids.map((id) => items.get(id))
        );
    };

    selectCompleteClusterConfiguration = ({clusterID, isDemo}) => (state$) => {
        const hasValues = (array) => !array.some((v) => !v);
        return state$.let(this.selectCluster(clusterID))
        .exhaustMap((cluster) => {
            if (!cluster)
                return of({__isComplete: false});

            const withSpace = (array) => array.map((c) => ({...c, space: cluster.space}));

            return Observable.forkJoin(
                state$.let(selectMapItems('caches', cluster.caches || [])).take(1),
                state$.let(selectMapItems('models', cluster.models || [])).take(1),
                state$.let(selectMapItems('igfss', cluster.igfss || [])).take(1),
            )
            .map(([caches, models, igfss]) => ({
                cluster,
                caches,
                domains: models,
                igfss,
                spaces: [{_id: cluster.space, demo: isDemo}],
                __isComplete: !!cluster && !(!hasValues(caches) || !hasValues(models) || !hasValues(igfss))
            }));
        });
    };
}
