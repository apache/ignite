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

import {Subject, merge, combineLatest} from 'rxjs';
import {tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';
import naturalCompare from 'natural-compare-lite';
import {removeClusterItems, advancedSaveCache} from '../../../configuration/store/actionCreators';
import ConfigureState from '../../../configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import Caches from '../../../configuration/services/Caches';
import Services from '../../services/Services';
import Version from 'app/services/Version.service';
import {ShortCache} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';
import AgentManager from 'app/modules/agent/AgentManager.service';
// Controller for Caches screen.
export default class CacheServiceController {
    static $inject = [
        'ConfigSelectors',
        'configSelectionManager',
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
        }
    ];

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
            this.callService('serviceList').then((data) => {
                if(data.message){
                    this.message = data.message;
                    return;
                }  
                this.serviceMap = Object.assign(data.result);
                Object.keys(this.serviceMap).forEach((key) => {
                   this.serviceList.push(this.serviceMap[key]);
                });
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

        this.subscription = merge(
            this.originalCache$,
            this.selectionManager.editGoes$.pipe(tap((id) => this.edit(id))),
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.cache-service.select', null, options)))
        ).subscribe();

        this.isBlocked$ = cacheID$;        

        this.serviceList$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => { return this.serviceList; }));
        
        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => [
            {
                action: 'Clone',
                click: () => this.clone(selectedItems),
                available: false
            },
            {
                action: 'Load Data',
                click: () => {
                    this.call(selectedItems,'loadDataService');
                },
                available: true
            },
            {
                action: 'Clear Data',
                click: () => {
                    this.call(selectedItems,'clearDataService');
                },
                available: true
            },
            {
                action: 'Write Data to Other Cluster',
                click: () => {
                    this.call(selectedItems,'writeDataService');
                },
                available: true
            }
        ]));
    }

    call(itemIDs: Array<string>, serviceName: string) {
       let cacheNames$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCacheNames(itemIDs));
       cacheNames$.subscribe((cacheNames)=>{
           this.callService(serviceName,{caches:cacheNames}).then((data) => {
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
           this.AgentManager.callClusterService({id:clusterID},serviceName,args).then((data) => {  
                
                if(data.result){
                    resolve(data);
                }    
                else if(data.message){
                    this.message = data.message;
                    resolve(data)
                }                 
            })   
           .catch((e) => {
                //this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e);    
                reject(e)       
            });
        });   
        
    }

    edit(cacheID: string) {
        this.$state.go('base.console.edit.cache-service.select', {cacheID});
    }

    onCall({cache, updated}) {
        return {id: this.clusterID, cache:cache, updated:updated};
    }
}
