

import {Subject, BehaviorSubject, merge, combineLatest, from, of, empty} from 'rxjs';
import {tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged} from 'rxjs/operators';
import {UIRouter, TransitionService, StateService} from '@uirouter/angularjs';
import naturalCompare from 'natural-compare-lite';
import {removeClusterItems, advancedSaveCache} from 'app/configuration/store/actionCreators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import ConfigSelectors from 'app/configuration/store/selectors';
import Caches from 'app/configuration/services/Caches';
import Services from 'app/console/services/Services';
import Version from 'app/services/Version.service';

import {IColumnDefOf} from 'ui-grid';
import AgentManager from 'app/modules/agent/AgentManager.service';
// Controller for service screen.
export default class ClusterControlController {
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

    clusterId: string;

    commandColumnDefs = [
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
            width: 150
        },
        {
            name: 'text',
            displayName: 'Description',
            field: 'text',
            enableHiding: false,
            sortingAlgorithm: naturalCompare,
            width: 350
        },
        {
            name: 'usage',
            displayName: 'Usage',
            field: 'usage',
            filter: {
                placeholder: 'Filter by usage...'
            }, 
            cellTemplate: `
                <div class="ui-grid-cell-contents" ng-bind-html="row.entity.usage"></div>
            `,            
            type: 'string',
            minWidth: 400
        },
        {
            name: 'experimental',
            displayName: 'Experimental',
            field: 'experimental', 
            enableFiltering: false,
            width: 150
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
        
        this.serviceMap = {};
        this.serviceList = [];
        
        this.clusterID = this.clusterId? this.clusterId: await clusterID$.toPromise();
        this.serviceList$ = from(this.callCommandList());         
        
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
            return `${isNew ? 'Deploy' : 'Select'} command ${!isNew && !!service && service.name ? `‘${service.name}’` : ''}`;
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
            this.selectionManager.editLeaves$.pipe(tap((options) => this.$state.go('base.console.edit.advanced.cluster', null, options)))
        ).subscribe();
    
        this.isBlocked$ = serviceID$; 
        
        this.tableActions$ = this.selectionManager.selectedItemIDs$.pipe(map((selectedItems) => [
            {
                action: 'Usage',
                click: () => {
                    this.callBatch(selectedItems,'usage');
                },
                available: false
            }
           
        ]));
    }

    $onDestroy() {
        this.subscription.unsubscribe();
        this.visibleRows$.complete();
        this.selectedRows$.complete();
    }
    
    callBatch(itemIDs: Array<string>, serviceName: string) {
       this.callCommand(serviceName,{commands:itemIDs}).then((data) => {
            if(data.message){
                this.message = data.message;
            }
       });  
    }
    
    callCommand(cmdName: string, args) {
        let clusterID = this.clusterID;
        return new Promise((resolve,reject) => {
           this.AgentManager.callClusterCommand({id:clusterID},cmdName,args).then((data) => {                
                if(data.message){   
                    this.message = data.message;                    
                }
                resolve(data)    
            })   
           .catch((e) => {                 
                reject(e)       
            });
        });   
        
    }
    
    callCommandList() {
        let clusterID = this.clusterID;
        return new Promise((resolve,reject) => {
           this.AgentManager.callClusterCommand({id:clusterID},'commandList').then((data) => {  
                if(data.result){
                    let serviceList = data.result;
                    let serviceMap = {};
                    serviceList.forEach((val) => {
                       let key:string = val['name']
                       val['id'] = key;
                       val['input'] = '--' + key.toLowerCase()
                       serviceMap[key] = val;                                          
                    });    
                    this.serviceMap = serviceMap;
                    resolve(serviceList);
                }
                else if(data.message){
                    this.message = data.message;                   
                }        
            })   
           .catch((e) => {                
                reject(e)       
            });
        });   
        
    }

    edit(serviceID: string) {
        this.$state.go('base.console.edit.advanced.cluster.command', {serviceID});
    }

    onCall({name, args}) {
        return {id: this.clusterID, cmdName:name};
    }
}
