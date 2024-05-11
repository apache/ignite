

import {Subject, from, merge, combineLatest} from 'rxjs';
import {tap, map, filter, refCount, pluck, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';
import naturalCompare from 'natural-compare-lite';

import ConfigureState from 'app/configuration/services/ConfigureState';
import ConfigSelectors from 'app/configuration/store/selectors';
import Caches from 'app/configuration/services/Caches';
import TaskFlows from 'app/console/services/TaskFlows';
import Version from 'app/services/Version.service';
import {ShortCache} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';

// Controller for Caches screen.
export default class Controller {
    static $inject = [
        'ConfigSelectors',
        'configSelectionManager',
        '$uiRouter',
        '$transitions',
        'ConfigureState',
        '$state',
        'IgniteVersion',
        'TaskFlows',
        'Caches'
    ];

    constructor(
        private ConfigSelectors,
        private configSelectionManager,
        private $uiRouter: UIRouter,
        private $transitions: TransitionService,
        private ConfigureState: ConfigureState,
        private $state: StateService,
        private Version: Version,
        private TaskFlows: TaskFlows,
        private Caches: Caches
    ) {}

    visibleRows$ = new Subject();
    selectedRows$ = new Subject();

    cachesColumnDefs: Array<IColumnDefOf<ShortCache>> = [
        {
            name: 'name',
            displayName: 'Name',
            field: 'name',
            enableHiding: false,
            sort: {direction: 'asc', priority: 0},
            filter: {
                placeholder: 'Filter by name…'
            },
            sortingAlgorithm: naturalCompare,
            minWidth: 165
        },
        {
            name: 'cacheMode',
            displayName: 'Mode',
            field: 'cacheMode',
            multiselectFilterOptions: this.Caches.cacheModes,
            width: 160
        },
        {
            name: 'atomicityMode',
            displayName: 'Atomicity',
            field: 'atomicityMode',
            multiselectFilterOptions: this.Caches.atomicityModes,
            width: 160
        },
        {
            name: 'backups',
            displayName: 'Backups',
            field: 'backups',
            width: 130,
            enableFiltering: false,
            cellTemplate: `
                <div class="ui-grid-cell-contents">{{ grid.appScope.$ctrl.Caches.getCacheBackupsCount(row.entity) }}</div>
            `
        }
    ];

    $onInit() {
        
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));        
                
        const cacheID$ = this.$uiRouter.globals.params$.pipe(
            pluck('cacheID'),
            publishReplay(1),
            refCount()
        );
          
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());  
        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);
        this.shortModels$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortModels);
        
        this.originalCluster$ = this.clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                this.clusterId = id;
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );

        this.originalCache$ = cacheID$.pipe(
            distinctUntilChanged(),
            filter((id) => id),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCacheToEdit(id));
            })
        );
        
        this.cacheDataProvider$ = this.originalCache$.pipe(            
            switchMap((cache) => {
                if(cache){
                    return this.TaskFlows.getTaskFlowsOfTarget(this.clusterId,cache.name);
                }
                return { data: [] }                   
            }),
            map((result) => { return result.data; })
            
        );

        this.isNew$ = cacheID$.pipe(map((id) => id === 'new'));
        this.itemEditTitle$ = combineLatest(this.isNew$, this.originalCache$, (isNew, cache) => {
            return `${isNew ? 'Create' : 'Edit'} cache ${!isNew && !!cache && cache.name ? `‘${cache.name}’` : ''}`;
        });
        this.selectionManager = this.configSelectionManager({
            itemID$: cacheID$,
            selectedItemRows$: this.selectedRows$,
            visibleRows$: this.visibleRows$,
            loadedItems$: this.shortCaches$
        });
       
        this.subscription = merge(
            this.originalCache$,
            this.selectionManager.editGoes$.pipe(tap((id) => this.edit(id))),
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.advanced.caches', null, options)))
        ).subscribe();
       
        this.isBlocked$ = cacheID$;

        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => [
            {
                action: 'Clone',
                click: () => this.clone(selectedItems),
                available: false
            },
            {
                action: 'Delete',
                click: () => {
                    this.remove(selectedItems);
                },
                available: true
            }
        ]));
        
        
    }

    remove(itemIDs: Array<string>) {
       // this.ConfigureState.dispatchAction(
            //removeClusterItems(this.$uiRouter.globals.params.clusterID, 'caches', itemIDs, true, true)
       // );
    }
    
    clone(itemIDs: Array<string>) {
       // this.ConfigureState.dispatchAction(
            //removeClusterItems(this.$uiRouter.globals.params.clusterID, 'caches', itemIDs, true, true)
       // );
    }

    $onDestroy() {
        this.subscription.unsubscribe();
        this.visibleRows$.complete();
        this.selectedRows$.complete();
    }

    edit(cacheID: string) {
        this.$state.go('base.console.edit.advanced.caches.cache', {cacheID});
    }

    save({cache, download}) {
        //this.ConfigureState.dispatchAction(advancedSaveCache(cache, download));
    }
}
