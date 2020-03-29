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
import {of, empty, combineLatest, forkJoin, pipe} from 'rxjs';
import {filter, pluck, map, switchMap, take, distinctUntilChanged, exhaustMap} from 'rxjs/operators';
import {defaultNames} from '../defaultNames';
import {DomainModel, ShortCluster} from '../types';

import {default as Caches} from '../services/Caches';
import {default as Clusters} from '../services/Clusters';
import {default as IGFSs} from '../services/IGFSs';
import {default as Models} from '../services/Models';

const isDefined = filter((v) => v);

const selectItems = (path) => pipe(filter((s) => s), pluck(path), filter((v) => v));

const selectValues = map((v) => v && [...v.value.values()]);

export const selectMapItem = (mapPath, key) => pipe(pluck(mapPath), map((v) => v && v.get(key)));

const selectMapItems = (mapPath, keys) => pipe(pluck(mapPath), map((v) => v && keys.map((key) => v.get(key))));

const selectItemToEdit = ({items, itemFactory, defaultName = '', itemID}) => switchMap((item) => {
    if (item)
        return of(Object.assign(itemFactory(), item));

    if (itemID === 'new')
        return items.pipe(take(1), map((items) => Object.assign(itemFactory(), {name: uniqueName(defaultName, items)})));

    if (!itemID)
        return of(null);

    return empty();
});

const currentShortItems = ({changesKey, shortKey}) => (state$) => {
    return combineLatest(
        state$.pipe(pluck('edit', 'changes', changesKey), isDefined, distinctUntilChanged()),
        state$.pipe(pluck(shortKey, 'value'), isDefined, distinctUntilChanged())
    ).pipe(
        map(([{ids = [], changedItems}, shortItems]) => {
            if (!ids.length || !shortItems)
                return [];

            return ids.map((id) => changedItems.find(({_id}) => _id === id) || shortItems.get(id));
        }),
        map((v) => v.filter((v) => v))
    );
};

const selectNames = (itemIDs, nameAt = 'name') => pipe(
    pluck('value'),
    map((items) => itemIDs.map((id) => items.get(id)[nameAt]))
);

export default class ConfigSelectors {
    static $inject = ['Caches', 'Clusters', 'IGFSs', 'Models'];

    /**
     * @param {Caches} Caches
     * @param {Clusters} Clusters
     * @param {IGFSs} IGFSs
     * @param {Models} Models
     */
    constructor(private Caches: Caches, private Clusters: Clusters, private IGFSs: IGFSs, private Models: Models) {}

    /**
     * @returns {(state$: Observable) => Observable<DomainModel>}
     */
    selectModel = (id: string) => selectMapItem('models', id);

    /**
     * @returns {(state$: Observable) => Observable<{pristine: boolean, value: Map<string, ShortDomainModel>}>}
     */
    selectShortModels = () => selectItems('shortModels');

    selectShortModelsValue = () => (state$) => state$.pipe(this.selectShortModels(), selectValues);

    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCluster>>}
     */
    selectShortClustersValue = () => (state$) => state$.pipe(this.selectShortClusters(), selectValues);

    /**
     * @returns {(state$: Observable) => Observable<Array<string>>}
     */
    selectClusterNames = (clusterIDs) => (state$) => state$.pipe(
        this.selectShortClusters(),
        selectNames(clusterIDs)
    );

    selectCluster = (id) => selectMapItem('clusters', id);

    selectShortClusters = () => selectItems('shortClusters');

    selectCache = (id) => selectMapItem('caches', id);

    selectIGFS = (id) => selectMapItem('igfss', id);

    selectShortCaches = () => selectItems('shortCaches');

    selectShortCachesValue = () => (state$) => state$.pipe(this.selectShortCaches(), selectValues);

    selectShortIGFSs = () => selectItems('shortIgfss');

    selectShortIGFSsValue = () => (state$) => state$.pipe(this.selectShortIGFSs(), selectValues);

    selectShortModelsValue = () => (state$) => state$.pipe(this.selectShortModels(), selectValues);

    selectCacheToEdit = (cacheID) => (state$) => state$.pipe(
        this.selectCache(cacheID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectCurrentShortCaches),
            itemFactory: () => this.Caches.getBlankCache(),
            defaultName: defaultNames.cache,
            itemID: cacheID
        })
    );

    selectIGFSToEdit = (itemID) => (state$) => state$.pipe(
        this.selectIGFS(itemID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectCurrentShortIGFSs),
            itemFactory: () => this.IGFSs.getBlankIGFS(),
            defaultName: defaultNames.igfs,
            itemID
        })
    );

    selectModelToEdit = (itemID) => (state$) => state$.pipe(
        this.selectModel(itemID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectCurrentShortModels),
            itemFactory: () => this.Models.getBlankModel(),
            itemID
        })
    );

    selectClusterToEdit = (clusterID, defaultName = defaultNames.cluster) => (state$) => state$.pipe(
        this.selectCluster(clusterID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectShortClustersValue()),
            itemFactory: () => this.Clusters.getBlankCluster(),
            defaultName,
            itemID: clusterID
        })
    );

    selectCurrentShortCaches = currentShortItems({changesKey: 'caches', shortKey: 'shortCaches'});

    selectCurrentShortIGFSs = currentShortItems({changesKey: 'igfss', shortKey: 'shortIgfss'});

    selectCurrentShortModels = currentShortItems({changesKey: 'models', shortKey: 'shortModels'});

    selectClusterShortCaches = (clusterID) => (state$) => {
        if (clusterID === 'new')
            return of([]);

        return combineLatest(
            state$.pipe(this.selectCluster(clusterID), pluck('caches')),
            state$.pipe(this.selectShortCaches(), pluck('value')),
            (ids, items) => ids.map((id) => items.get(id))
        );
    };

    selectCompleteClusterConfiguration = ({clusterID, isDemo}) => (state$) => {
        const hasValues = (array) => !array.some((v) => !v);
        return state$.pipe(
            this.selectCluster(clusterID),
            exhaustMap((cluster) => {
                if (!cluster)
                    return of({__isComplete: false});

                return forkJoin(
                    state$.pipe(selectMapItems('caches', cluster.caches || []), take(1)),
                    state$.pipe(selectMapItems('models', cluster.models || []), take(1)),
                    state$.pipe(selectMapItems('igfss', cluster.igfss || []), take(1)),
                ).pipe(map(([caches, models, igfss]) => ({
                    cluster,
                    caches,
                    domains: models,
                    igfss,
                    spaces: [{_id: cluster.space, demo: isDemo}],
                    __isComplete: !!cluster && !(!hasValues(caches) || !hasValues(models) || !hasValues(igfss))
                })));
            })
        );
    };
}
