import {Subject, Observable, from, of} from 'rxjs';
import {map,switchMap, distinctUntilChanged, catchError} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

import {UIRouter} from '@uirouter/angularjs';
import {ShortCluster} from '../../types';
import Datasource from 'app/datasource/services/Datasource';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {IColumnDefOf} from 'ui-grid';
import {dbPresets} from 'app/datasource/dbPresets';
//-import crudPage from './crud-list.json';

const cellTemplate = (state) => `
    <div class="ui-grid-cell-contents">
        <a
            class="link-success"
            ui-sref="base.datasource.edit.basic({clusterID: row.entity.id})"
            title='Click to edit'
        >{{ 'Edit' }}</a>
    </div>
`;

export default class PageDatasourceOverviewController {
    static $inject = [
        '$uiRouter',
        'ConfigureState',
        'AgentManager',
        'Datasource'        
    ];
    

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private AgentManager: AgentManager,
        private Datasource: Datasource    
    ) {}

    shortClusters$: Observable<Array<ShortCluster>>;    
    selectedRows$: Subject<Array<ShortCluster>>;
    selectedRowsIDs$: Observable<Array<string>>;
    
    
    datasourceColumnDefs: Array<any> = [
        {
            name: 'jndiName',
            displayName: 'JNDI Name',
            field: 'jndiName',
            enableHiding: false,
            enableFiltering: true,
            sort: {direction: 'asc'},
            sortingAlgorithm: naturalCompare,            
            width: 180
        },
        {
            name: 'jdbcUrl',
            displayName: 'JDBC URL',
            field: 'jdbcUrl',
            filter: {
                placeholder: 'Filter by keyword…'
            },
            sort: {direction: 'asc'},
            sortingAlgorithm: naturalCompare,
            minWidth: 350
        },
        {
            name: 'driverCls',
            displayName: 'driverClass',
            field: 'driverCls',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,
            type: 'number',
            width: 180
        },
        {
            name: 'schemaName',
            displayName: 'Schema',
            field: 'schemaName',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,
            type: 'number',
            width: 150
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
            width: 75
        },
        {
            name: 'id',
            displayName: 'Action',
            field: 'id',
            cellClass: 'ui-grid-number-cell',                
            cellTemplate: cellTemplate,
            enableFiltering: false,
            type: 'string',
            width: 75
        }
    ];    


    editDatasource(cluster) {
        return this.$uiRouter.stateService.go('^.edit', {clusterID: cluster.id});
    }
    
    pingDatasource(clusters: Array<any>) {
      for(let cluster of clusters){
         this.AgentManager.callClusterService(cluster,'datasourceTest',cluster).then((msg) => {
             if(msg.status){
                cluster.status = msg.status;               
             }      
             
         });         
      }
    }

    $onInit() {
        
        /**
        
        this.amis = amisRequire('amis/embed');
        let drivers = [];
        for(let engine of dbPresets){
            let option = {"label": engine.db, "value": engine.jdbcDriverClass}
            drivers.push(option);
        }
        
        let amisJSON = crudPage;
        amisJSON.data = {};
        amisJSON.data.drivers = drivers
        let amisScoped = this.amis.embed('#amis_root', amisJSON);
        */
       
       
       
        this.dataSourceList$ = from(this.Datasource.getDatasourceList()).pipe(            
            switchMap(({data}) => of(
                data            
            )),
            catchError((error) => of({
                type: `DATASOURCE_ERR`,
                error: {
                    message: `Failed to load datasoure:  ${error.data.message}`
                },
                action: {}
            }))           
        );
        
        this.selectedRows$ = new Subject();
        
        this.selectedRowsIDs$ = this.selectedRows$.pipe(map((selectedDatasources) => selectedDatasources.map((cluster) => cluster.id)));
        
        this.actions$ = this.selectedRows$.pipe(map((selectedDatasource) => [
            {
                action: 'Ping',
                click: () => this.pingDatasource(selectedDatasource),
                available: selectedDatasource.length >= 1
            }     
        ]));
    }
    
    $onDestroy() {
        this.selectedRows$.complete();
    }
}
