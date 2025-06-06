

import {Subject, merge, combineLatest} from 'rxjs';
import {tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';
import naturalCompare from 'natural-compare-lite';
import ConfigureState from '../../../configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import {default as MessagesFactory} from '../../../services/Messages.service';
import Caches from '../../../configuration/services/Caches';
import Services from '../../services/Services';
import Version from 'app/services/Version.service';
import {ShortCache} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';
import AgentManager from 'app/modules/agent/AgentManager.service';
import CacheMetrics from 'app/modules/cluster/CacheMetrics';

// Controller for Caches screen.
export default class CacheServiceController {
    static $inject = [
        'ConfigSelectors',
        'configSelectionManager',
        'IgniteMessages',
        '$uiRouter',
        '$transitions',
        'ConfigureState',
        '$state',
        'IgniteVersion',
        'AgentManager',
        'Services',
        'Caches'
    ];

    constructor(
        private ConfigSelectors,
        private configSelectionManager,
        private messages: ReturnType<typeof MessagesFactory>,
        private $uiRouter: UIRouter,
        private $transitions: TransitionService,
        private ConfigureState: ConfigureState,
        private $state: StateService,
        private Version: Version,
        private AgentManager: AgentManager,
        private Services: Services,
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
        },
        {   
            name: 'rows',
            displayName: 'Rows/Size',
            field: 'rows',
            width: 160,
            enableFiltering: false,
            cellTemplate: `
                <div class="ui-grid-cell-contents">{{ grid.appScope.$ctrl.getCacheSize(row.entity) }}</div>
            `
        }
    ];

    cacheMetrics = {};

    $onInit() {
        const cacheID$ = this.$uiRouter.globals.params$.pipe(
            pluck('cacheID'),
            publishReplay(1),
            refCount()
        );
       
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
        
        this.serviceMap = {'status':{ id: 'status', name:'status', description:'get cluster last status', mode: 'NodeSinger'}};
        this.serviceList = [];        
        
        clusterID$.subscribe((v)=>{
            this.clusterID = v; 
            this.callService('serviceList',{type:'CacheAgentService'}).then((data) => {
                if(data.message){
                    this.message = data.message;                    
                }
                if(data.result){
                	this.serviceMap = Object.assign(data.result);
	                Object.keys(this.serviceMap).forEach((key) => {
	                   this.serviceList.push(this.serviceMap[key]);
	                });
                }
                              
            });

            this.callService('CacheMetricsService',{}).then((m)=>{
                if(m){
                    this.cacheMetrics = m.result;                    
                } 
            });
            
        });
        
        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);
        this.shortModels$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortModels);
        this.originalCache$ = cacheID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCacheToEdit(id));
            })
        );

        this.isNew$ = cacheID$.pipe(map((id) => id === 'new'));
        this.itemEditTitle$ = combineLatest(this.isNew$, this.originalCache$, (isNew, cache) => {
            return `${isNew ? 'Create' : 'Select'} cache ${!isNew && !!cache && cache.name ? `‘${cache.name}’` : ''}`;
        });
        this.selectionManager = this.configSelectionManager({
            itemID$: cacheID$,
            selectedItemRows$: this.selectedRows$,
            visibleRows$: this.visibleRows$,
            loadedItems$: this.shortCaches$
        });        

        this.isBlocked$ = cacheID$;        
        // 根据caches选择可以执行的services
        this.serviceList$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => { return this.serviceList; }));
        
        this.subscription = merge(
            this.originalCache$,
            this.serviceList$,
            this.selectionManager.editGoes$.pipe(tap((id:string) => this.edit(id))),
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.cache-service.select', null, options)))
        ).subscribe();
        
        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems: Array<string>) => [
            {
                action: 'Flush Data', 
                click: () => {
                    this.call(selectedItems,'CacheSaveDataService');
                },
                available: true
            },
            {
                action: 'Load Data',
                click: () => {
                    this.call(selectedItems,'CacheLoadDataService');
                },
                available: true
            },
            {
                action: 'Clear Data',
                click: () => {
                    this.call(selectedItems,'CacheClearDataService');
                },
                available: true
            },
            {
                action: 'Poll Data from Other Cluster',
                click: () => {
                    this.call(selectedItems,'CacheCopyDataService');
                },
                available: true
            }
        ]));
    }

    getCacheSize(cache) {
        const m = cache && this.cacheMetrics[cache.name];
        if(m){
            return ''+m.cacheSize+'/'+ m.offHeapAllocatedSize;
        }             
        else{
            return '';
        }
    }
    getCacheMetrics(cache) {
        const m = cache && this.cacheMetrics[cache.name];
        if(m){
            return new CacheMetrics(cache,m);
        }             
        else{
            return '';
        }
    }

    call(itemIDs: Array<string>, serviceName: string) {
       console.log('call '+itemIDs);       
       let cacheNames$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCacheNames(itemIDs),take(1));
       cacheNames$.subscribe((cacheNames)=>{
           this.callService(serviceName,{caches:cacheNames,clusterId: this.clusterID}).then((data) => {
                if(data.message){
                    this.message = data.message;
                }
           });  
       });
    }

    $onDestroy() {
        this.subscription.unsubscribe();
        this.visibleRows$.complete();
        this.selectedRows$.complete();
    }
    
    callService(serviceName: string, args) {
        let clusterID = this.clusterID;
        return new Promise((resolve,reject) => {
           this.AgentManager.callCacheService({id:clusterID},serviceName,args).then((data) => {                    
                if(data.message){                    
                    this.messages.showInfo(data.message);                    
                }
                resolve(data);         
            })   
           .catch((e) => {
                this.messages.showError('Failed to callClusterService : '+serviceName+' Caused : '+e);    
                reject(e)       
            });
        });   
        
    }

    edit(cacheID: string) {
        this.$state.go('base.console.edit.cache-service.select', {cacheID});
    }

    onCall({cache, updated}) {
        return {clusterId: this.clusterID, cache:cache, updated:updated};
    }
}
