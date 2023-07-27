

import {Subject, Observable, combineLatest, from, of} from 'rxjs';
import {count,tap, map, refCount, pluck, take, filter, publishReplay, switchMap, distinctUntilChanged, catchError} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

import {default as ConfigureState} from 'app/configuration/services/ConfigureState';
import {default as ConfigSelectors} from 'app/configuration/store/selectors';
import {default as Clusters} from 'app/configuration/services/Clusters';
import TaskFlows from 'app/console/services/TaskFlows';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {Confirm} from 'app/services/Confirm.service';

import {UIRouter} from '@uirouter/angularjs';
import {ShortCluster,ShortCache} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';

export default class ClusterTaskFlowController {
    static $inject = [
        '$uiRouter',
        'Confirm',
        'Clusters',
        'TaskFlows',
        'ConfigureState',
        'AgentManager',
        'ConfigSelectors'        
    ];    

    constructor(
        private $uiRouter: UIRouter,
        private Confirm: Confirm,
        private Clusters: Clusters,
        private TaskFlows: TaskFlows,
        private ConfigureState: ConfigureState,
        private AgentManager: AgentManager, 
        private ConfigSelectors: ConfigSelectors       
    ) {
        
    }

    shortClusters$: Observable<Array<ShortCluster>>;    
    selectedRows$: Subject<Array<ShortCluster>>;
    selectedRowsIDs$: Observable<Array<string>>;
    
    targetCaches: Subject<Array<ShortCache>>; // user selected caches    
    models: Array<object>; // cluster defined models
    
    clustersColumnDefs: Array<any> = [
        {
            name: 'name',
            displayName: 'Name',
            field: 'name',
            enableHiding: false,
            filter: {
                placeholder: 'Filter by name…'
            },
            sort: {direction: 'asc', priority: 0},
            sortingAlgorithm: naturalCompare,            
            minWidth: 165
        },
        {
            name: 'discovery',
            displayName: 'Discovery',
            field: 'discovery',
            enableFiltering: false,
            width: 150
        },
        {
            name: 'caches',
            displayName: 'Caches',
            field: 'cachesCount',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,
            type: 'number',
            width: 95
        },
        {
            name: 'models',
            displayName: 'Models',
            field: 'modelsCount',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,
            type: 'number',
            width: 95
        },
        {
            name: 'status',
            displayName: 'Status',
            field: 'status',
            cellClass: 'ui-grid-number-cell',                
            cellTemplate: `
                <div class="ui-grid-cell-contents status-{{ row.entity.status }} ">{{ row.entity.status }}</div>
            `,
            enableFiltering: false,
            type: 'string',
            width: 95
        }
    ];
    
    pingClusters(clusters: Array<ShortCluster>) {
      for(let cluster of clusters){
         this.AgentManager.callClusterService(cluster,'serviceList').then((msg) => {
             if(msg.status){
                cluster.status = msg.status;
             }        
             
         });         
      }
    }

    editCluster(cluster: ShortCluster) {
        return this.$uiRouter.stateService.go('^.edit', {clusterID: cluster.id});
    }

    loadUserClusters(a: any) {
        return this.Clusters.getClustersOverview().pipe(
            switchMap(({data}) => of(
                {type: shortClustersActionTypes.SET, items: data},
                {type: `${a.type}_OK`}
            )),
            catchError((error) => of({
                type: `${a.type}_ERR`,
                error: {
                    message: `Failed to load clusters:  ${error.data.message}`
                },
                action: a
            }))
        );        
    }
    
    ngAfterContentInit(){
        
    }
    
    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(           
            pluck('clusterID')           
        );
         
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());
        
        this.selectedRows$ = new Subject();

        this.selectedRowsIDs$ = this.selectedRows$.pipe(map((selectedClusters) => selectedClusters.map((cluster) => cluster.id)));

        this.actions$ = this.selectedRows$.pipe(map((selectedClusters) => [
            {
                action: 'Ping',
                click: () => this.pingClusters(selectedClusters),
                available: true
            },
            {
                action: 'Start',
                click: () => this.editCluster(selectedClusters[0]),
                available: selectedClusters.length === 1
            }           
        ]));
        
        this.isBlocked$ = this.selectedRowsIDs$.pipe(count()); 
        
        this.itemEditTitle$ = combineLatest(this.selectedRows$, (clusters) => {
            let names = 'Select cluster ';
            for(let cluster  of clusters){
                names+=' '+cluster.name
            }
            return names;
        });
        
    }
    
    $onDestroy() {
        this.selectedRows$.complete();
    }
    
        
    onSave(event) {
        this.saved = true;        
        return event;
    }
}
