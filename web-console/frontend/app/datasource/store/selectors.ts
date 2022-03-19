/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import uuidv4 from 'uuid/v4';

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

            return ids.map((item) => changedItems.find(({id}) => item === id) || shortItems.get(item));
        }),
        map((v) => v.filter((v) => v))
    );
};

const selectNames = (itemIDs, nameAt = 'name') => pipe(
    pluck('value'),
    map((items) => itemIDs.map((id) => items.get(id)[nameAt]))
);

function getBlankDatasource() {
        return {
            name: '',
            id: uuidv4()
        }
 }
 
export default class DatasourceSelectors {
    static $inject = [];

    /**
     * @param {Caches} Caches
     * @param {Clusters} Clusters
     * @param {Models} Models
     */
    constructor() {}


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



    selectClusterToEdit = (clusterID, defaultName = '数据源') => (state$) => state$.pipe(
        this.selectCluster(clusterID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectShortClustersValue()),
            itemFactory: () => getBlankDatasource(),
            defaultName,
            itemID: clusterID
        })
    );

    
}
