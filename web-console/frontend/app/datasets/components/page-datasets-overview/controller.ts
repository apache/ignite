import {Subject, Observable, from, of} from 'rxjs';
import {map,switchMap, distinctUntilChanged, catchError} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

import {UIRouter} from '@uirouter/angularjs';

import ConfigureState from 'app/configuration/services/ConfigureState';
import {default as ConfigSelectors} from 'app/configuration/store/selectors';
import {default as Clusters} from 'app/configuration/services/Clusters';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {ShortCluster} from 'app/configuration/types';
import {IColumnDefOf} from 'ui-grid';

const cellTemplateGo = (state) => `
    <div class="ui-grid-cell-contents">
        <a
            class="link-success"
            ui-sref="${state}({datasetID: row.entity.id})"
            title='Click to Visit'
        >{{ 'CRUD UI' }}</a>
    </div>
`;

const cellTemplate = (state) => `
    <div class="ui-grid-cell-contents">
        <a
            class="link-success"
            ui-sref="${state}({clusterID: row.entity.id})"
            title='Click to edit'
        >{{ row.entity[col.field] }}</a>
    </div>
`;


export default class PageDatasourceOverviewController {
    static $inject = [
        '$uiRouter',
        'ConfigureState',
        'AgentManager',
        'Clusters',
        'ConfigSelectors',
    ];
    

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private AgentManager: AgentManager,
        private Clusters: Clusters,
        private ConfigSelectors: ConfigSelectors,
    ) {}

    shortClusters$: Observable<Array<ShortCluster>>;
    clustersColumnDefs: Array<IColumnDefOf<ShortCluster>>;
    selectedRows$: Subject<Array<Clusters>>;
    selectedRowsIDs$: Observable<Array<string>>;

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
                cellTemplate: cellTemplate('base.configuration.edit'),
                width: 200
            },
            {
                name: 'comment',
                displayName: 'Comment',
                field: 'comment',
                enableHiding: false,
                filter: {
                    placeholder: 'Filter by comment…'
                },
                sort: {direction: 'asc', priority: 0},
                sortingAlgorithm: naturalCompare,
                cellTemplate: cellTemplate('base.configuration.edit.basic'),
                minWidth: 200
            },
            {
                name: 'id',
                displayName: 'Action',
                field: 'id',
                enableHiding: false,
                enableFiltering: false,
                cellTemplate: cellTemplateGo('base.datasets.edit.basic'),
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
                cellTemplate: cellTemplate('base.configuration.edit.advanced.caches'),
                enableFiltering: false,
                type: 'number',
                width: 95
            },
            {
                name: 'models',
                displayName: 'Models',
                field: 'modelsCount',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.configuration.edit.advanced.models'),
                enableFiltering: false,
                type: 'number',
                width: 95
            },
            {
                name: 'status',
                displayName: 'Status',
                field: 'status',
                cellClass: 'ui-grid-number-cell',
                cellTemplate: cellTemplate('base.configuration.edit.basic.models'),
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
        ]));
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
    
    $onDestroy() {
        this.selectedRows$.complete();
    }
}
