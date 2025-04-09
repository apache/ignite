

import {Subject, Observable} from 'rxjs';
import {map} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

const cellTemplate = (state) => `
    <div class="ui-grid-cell-contents">
        <a
            class="link-success"
            ui-sref="${state}({clusterID: row.entity.id})"
            title='Click to edit'
        >{{ row.entity[col.field] }}</a>
    </div>
`;

import {default as ConfigureState} from 'app/configuration/services/ConfigureState';
import {default as ConfigSelectors} from 'app/configuration/store/selectors';
import {default as Clusters} from 'app/configuration/services/Clusters';
import {Confirm} from 'app/services/Confirm.service';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {UIRouter} from '@uirouter/angularjs';
import {ShortCluster} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';

export default class PageConfigureOverviewController {
    static $inject = [
        '$uiRouter',
        'Confirm',
        'Clusters',
        'ConfigureState',
        'AgentManager',
        'ConfigSelectors'        
    ];

    constructor(
        private $uiRouter: UIRouter,
        private Confirm: Confirm,
        private Clusters: Clusters,
        private ConfigureState: ConfigureState,
        private AgentManager: AgentManager, 
        private ConfigSelectors: ConfigSelectors       
    ) {}

    shortClusters$: Observable<Array<ShortCluster>>;
    clustersColumnDefs: Array<IColumnDefOf<ShortCluster>>;
    selectedRows$: Subject<Array<ShortCluster>>;
    selectedRowsIDs$: Observable<Array<string>>;

    $onDestroy() {
        this.selectedRows$.complete();
    }

    stopClusters(clusters: Array<ShortCluster>) {
        return this.Confirm.confirm(`
            <p>Are you sure you want to stop these clusters?</p>
            <ul>${clusters.map((cluster) => `<li>${cluster.name}</li>`).join('')}</ul>`)
            .then(() => this.doStopClusters(clusters))
            .catch(() => {});        
    }
    
    doStopClusters(clusters: Array<ShortCluster>) {
      for(let cluster of clusters){
         this.AgentManager.stopCluster(cluster).then((msg) => {
             if(msg.status){               
                this.ConfigureState.dispatchAction({type: 'STOP_CLUSTER'});
                cluster.status = msg.status;
             }        
             
         });         
      }
    }
    
    pingClusters(clusters: Array<ShortCluster>) {
      for(let cluster of clusters){
         this.AgentManager.callClusterService(cluster,'serviceList',{}).then((msg) => {
             if(msg.status){
                cluster.status = msg.status;
             }        
             
         });         
      }
    }

    editCluster(cluster: ShortCluster) {
        return this.$uiRouter.stateService.go('^.edit', {clusterID: cluster.id});
    }

    $onInit() {
        this.shortClusters$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectShortClustersValue());

        this.clustersColumnDefs = [
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
                cellTemplate: cellTemplate('base.console.edit'),
                minWidth: 165
            },
            {
                name: 'discovery',
                displayName: 'Discovery',
                field: 'discovery',
                multiselectFilterOptions: this.Clusters.discoveries,
                width: 200
            },
            {
                name: 'caches',
                displayName: 'Caches',
                field: 'cachesCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.console.edit.advanced.caches'),
                enableFiltering: false,
                type: 'number',
                width: 95
            },
            {
                name: 'models',
                displayName: 'Models',
                field: 'modelsCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.console.edit.advanced.models'),
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
            },
            {
                action: 'Stop',
                click: () => this.stopClusters(selectedClusters),
                available: true
            },            
           
        ]));
    }
}
