
import {uniqueName} from 'app/utils/uniqueName';
import {Observable, of, empty, combineLatest, forkJoin, pipe} from 'rxjs';
import {filter, pluck, map, switchMap, take, distinctUntilChanged, exhaustMap} from 'rxjs/operators';
import {defaultNames} from '../defaultNames';

import {default as Caches} from '../services/Caches';
import {default as Clusters} from '../services/Clusters';
import {default as IGFSs} from '../services/IGFSs';
import {default as Models} from '../services/Models';
import {Cluster,Cache,ShortCache,DomainModel,ShortDomainModel,ShortIGFS} from '../types';

const isDefined = filter((v:any) => v);

const selectItems = (path:string) => pipe(filter((s:any) => s), pluck(path), filter((v) => v));


const selectValues = map((v:{ value: Map<any,any> }) => v && [...v.value.values()]);

export const selectMapItem = (mapPath:string, key:string) => pipe(pluck(mapPath), map((v:Map<any,any>) => v && v.get(key)));

const selectMapItems = (mapPath:string, keys:string[]) => pipe(pluck(mapPath), map((v:Map<any,any>) => v && keys.map((key) => v.get(key))));

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
        map(([{ids=[], changedItems=[]}, shortItems]) => {
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

     /**
     * @returns {(state$: Observable) => Observable<Array<ShortDomainModel>>}
     */
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

    /**
     * @returns {(state$: Observable) => Observable<Cluster>}
     */
    selectCluster = (id) => selectMapItem('clusters', id);

    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCluster>>}
     */
    selectShortClusters = () => selectItems('shortClusters');

    /**
     * @returns {(state$: Observable) => Observable<Cache>}
     */
    selectCache = (id) => selectMapItem('caches', id);

    /**
     * @returns {(state$: Observable) => Observable<ShortIGFS>}
     */
    selectIGFS = (id) => selectMapItem('igfss', id);
    

     /**
     * @returns {(state$: Observable) => Observable<{pristine: boolean, value: Map<string, ShortCache>}>}
     */
    selectShortCaches = () => selectItems('shortCaches');

    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCache>>}
     */
    selectShortCachesValue = () => (state$) => state$.pipe(this.selectShortCaches(), selectValues);
    
     /**
     * @returns {(state$: Observable) => Observable<{pristine: boolean, value: Map<string, ShortIGFS>}>}
     */
    selectShortIGFSs = () => selectItems('shortIgfss');
    selectShortIGFSsValue = () => (state$) => state$.pipe(this.selectShortIGFSs(), selectValues);

   /**
     * @returns {(state$: Observable) => Observable<Array<string>>}
     */
    selectCacheNames = (cacheIDs) => (state$) => state$.pipe(
        this.selectShortCaches(),
        selectNames(cacheIDs)
    );
    
     /**
     * @returns {(state$: Observable) => Observable<Cache>}
     */
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
    
     /**
     * @returns {(state$: Observable) => Observable<IGFS>}
     */
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
     /**
     * @returns {(state$: Observable) => Observable<DomainModel>}
     */
    selectModelToEdit = (itemID) => (state$) => state$.pipe(
        this.selectModel(itemID),
        distinctUntilChanged(),
        selectItemToEdit({
            items: state$.pipe(this.selectCurrentShortModels),
            itemFactory: () => this.Models.getBlankModel(),
            itemID
        })
    );

    /**
     * @returns {(state$: Observable) => Observable<Cluster>}
     */
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

    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCache>>}
     */
    selectCurrentShortCaches = currentShortItems({changesKey: 'caches', shortKey: 'shortCaches'});
    /**
     * @returns {(state$: Observable) => Observable<Array<ShortIGFS>>}
     */
    selectCurrentShortIGFSs = currentShortItems({changesKey: 'igfss', shortKey: 'shortIgfss'});
    /**
     * @returns {(state$: Observable) => Observable<Array<ShortDomainModel>>}
     */
    selectCurrentShortModels = currentShortItems({changesKey: 'models', shortKey: 'shortModels'});

    /**
     * @returns {(state$: Observable) => Observable<Array<ShortCache>>}
     */
    selectClusterShortCaches = (clusterID) => (state$) => {
        if (clusterID === 'new')
            return of([]);

        return combineLatest(
            state$.pipe(this.selectCluster(clusterID), pluck('caches')),
            state$.pipe(this.selectShortCaches(), pluck('value')),
            (ids:Array<string>, items:any) => ids.map((id) => items.get(id))
        );
    };

    /**
     * @returns {(state$: Observable) => Observable<{cluster:Cluster,caches:Array<Cache>,domains:Array<DomainModel>,spaces:Array<object>}>}
     */
    selectCompleteClusterConfiguration = ({clusterID, isDemo}) => (state$) => {
        const hasValues = (array) => !array.some((v) => !v);
        return state$.pipe(
            this.selectCluster(clusterID),
            exhaustMap((cluster:Cluster) => {
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
                    spaces: [{id: cluster.space, demo: isDemo}],
                    __isComplete: !!cluster && !(!hasValues(caches) || !hasValues(models) || !hasValues(igfss))
                })));
            })
        );
    };
}
