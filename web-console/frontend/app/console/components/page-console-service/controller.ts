

import {Subject, BehaviorSubject, merge, combineLatest, from, of, empty} from 'rxjs';
import {tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';
import naturalCompare from 'natural-compare-lite';
import {removeClusterItems, advancedSaveCache} from '../../../configuration/store/actionCreators';
import ConfigureState from '../../../configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import Caches from '../../../configuration/services/Caches';
import Services from '../../services/Services';
import Version from 'app/services/Version.service';

import {IColumnDefOf} from 'ui-grid';
import AgentManager from 'app/modules/agent/AgentManager.service';
// Controller for service screen.
export default class ServiceController {
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

    serviceColumnDefs = [
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
            minWidth: 150
        },
        {
            name: 'description',
            displayName: 'Description',
            field: 'description',
            enableHiding: false,           
            filter: {
                placeholder: 'Filter by description…'
            },
            sortingAlgorithm: naturalCompare,
            width: 300
        },
        {
            name: 'mode',
            displayName: 'Mode',
            field: 'mode',
            multiselectFilterOptions: this.Services.serviceModes,
            width: 150
        },
        {
            name: 'cacheName',
            displayName: 'CacheName',
            field: 'cacheName',
            enableFiltering: false,
            width: 150
        },
        {
            name: 'affinityKey',
            displayName: 'affinityKey',
            field: 'affinityKey',
            enableFiltering: false,
            width: 150
        },
        {
           name: 'totalCount',
           displayName: 'totalCount',
           field: 'totalCount',
           enableFiltering: false,
           width: 80
        },
        {
           name: 'maxPerNodeCount',
           displayName: 'maxPerNodeCount',
           field: 'maxPerNodeCount',
           enableFiltering: false,
           width: 80
        }      
    ];


    async  $onInit() {
        const serviceID$ = this.$uiRouter.globals.params$.pipe(
            pluck('serviceID'),
            publishReplay(1),
            refCount()
        );
       
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );
       
        
        
        this.serviceMap = {
            'status':{ id: 'status', name:'status', description:'get cluster last status', mode: 'NodeSinger'},
            'serviceList':{ id: 'serviceList', name:'serviceList', description:'get cluster service list', mode: 'NodeSinger'}
        };
        this.serviceList = [this.serviceMap['status'],this.serviceMap['serviceList']];  
        
        
        //this.serviceListSubject$.next(this.serviceList);     
        
        this.clusterID = await clusterID$.toPromise();
        this.serviceList$ = from(this.callServiceList());
         
        
        this.originalService$ = serviceID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                if(id in this.serviceMap){
                    return of(this.serviceMap[id]);
                }
                return empty();
            })
        );

        this.isNew$ = serviceID$.pipe(map((id) => id === 'new'));
        this.itemEditTitle$ = combineLatest(this.isNew$, this.originalService$, (isNew, service) => {
            return `${isNew ? 'Deploy' : 'Select'} service ${!isNew && !!service && service.name ? `‘${service.name}’` : ''}`;
        });
        this.selectionManager = this.configSelectionManager({
            itemID$: serviceID$,
            selectedItemRows$: this.selectedRows$,
            visibleRows$: this.visibleRows$,
            loadedItems$: this.serviceList$
        });

        this.subscription = merge(
            this.originalService$,
            this.selectionManager.editGoes$.pipe(tap((id) => this.edit(id))),
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.service.select', null, options)))
        ).subscribe();

        this.isBlocked$ = serviceID$; 
        
        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => [
            
            {
                action: 'Redeploy',
                click: () => {
                    this.call(selectedItems,'redeployService');
                },
                available: true
            },
            {
                action: 'Undeploy',
                click: () => {
                    this.call(selectedItems,'undeployService');
                },
                available: true
            },
            {
                action: 'Simple Call',
                click: () => {
                    for(let serviceName of selectedItems){
                        this.callService(serviceName,{}).then((data) => {
                             if(data.message){
                                 this.message = data.message;
                             }
                        });  
                    }                   
                },
                available: true
            }
        ]));
    }

    call(itemIDs: Array<string>, serviceName: string) {
       this.callService(serviceName,{services:itemIDs}).then((data) => {
            if(data.message){
                this.message = data.message;
            }
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
    
    callServiceList() {
        let clusterID = this.clusterID;
        return new Promise((resolve,reject) => {
           this.AgentManager.callClusterService({id:clusterID},'serviceList').then((data) => {  
                if(data.result){
                    let serviceList = [];
                    let serviceMap = Object.assign(data.result);
                    Object.keys(serviceMap).forEach((key) => {
                       serviceMap[key]['id'] = key;
                       serviceList.push(serviceMap[key]);                       
                    });    
                    this.serviceMap = serviceMap;
                    resolve(serviceList);
                }  
               else if(data.message){
                   this.message = data.message;                   
               }        
            })   
           .catch((e) => {
                //this.$scope.message = ('Failed to callClusterService : '+serviceName+' Caused : '+e);    
                reject(e)       
            });
        });   
        
    }

    edit(serviceID: string) {
        this.$state.go('base.console.edit.service.select', {serviceID});
    }

    onCall({name, updated}) {
        return {id: this.clusterID, serviceName:name, updated:updated};
    }
}
