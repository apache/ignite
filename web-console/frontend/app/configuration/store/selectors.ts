

import {uniqueName} from 'app/utils/uniqueName';
import {of, empty, combineLatest, forkJoin, pipe} from 'rxjs';
import {filter, pluck, map, switchMap, take, distinctUntilChanged, exhaustMap} from 'rxjs/operators';
import {defaultNames} from '../defaultNames';

import {default as Caches} from '../services/Caches';
import {default as Clusters} from '../services/Clusters';
import {default as Models} from '../services/Models';
import {Cluster} from '../types';

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
    map((items:any) => itemIDs.map((id) => items.get(id)[nameAt]))
);

export default class ConfigSelectors {
    static $inject = ['Caches', 'Clusters', 'Models'];

    /**
     * @param {Caches} Caches
     * @param {Clusters} Clusters
     * @param {Models} Models
     */
    constructor(private Caches: Caches, private Clusters: Clusters, private Models: Models) {}

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

    selectShortCaches = () => selectItems('shortCaches');

    selectShortCachesValue = () => (state$) => state$.pipe(this.selectShortCaches(), selectValues);

   /**
     * @returns {(state$: Observable) => Observable<Array<string>>}
     */
    selectCacheNames = (cacheIDs) => (state$) => state$.pipe(
        this.selectShortCaches(),
        selectNames(cacheIDs)
    );
    
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

    selectCurrentShortModels = currentShortItems({changesKey: 'models', shortKey: 'shortModels'});

    selectClusterShortCaches = (clusterID) => (state$) => {
        if (clusterID === 'new')
            return of([]);

        return combineLatest(
            state$.pipe(this.selectCluster(clusterID), pluck('caches')),
            state$.pipe(this.selectShortCaches(), pluck('value')),
            (ids:Array<string>, items:any) => ids.map((id) => items.get(id))
        );
    };

    selectCompleteClusterConfiguration = ({clusterID, isDemo}) => (state$) => {
        const hasValues = (array) => !array.some((v) => !v);
        return state$.pipe(
            this.selectCluster(clusterID),
            exhaustMap((cluster:Cluster) => {
                if (!cluster)
                    return of({__isComplete: false});

                return forkJoin(
                    state$.pipe(selectMapItems('caches', cluster.caches || []), take(1)),
                    state$.pipe(selectMapItems('models', cluster.models || []), take(1))
                ).pipe(map(([caches, models]) => ({
                    cluster,
                    caches,
                    domains: models,
                    spaces: [{id: cluster.space, demo: isDemo}],
                    __isComplete: !!cluster && !(!hasValues(caches) || !hasValues(models))
                })));
            })
        );
    };
}
