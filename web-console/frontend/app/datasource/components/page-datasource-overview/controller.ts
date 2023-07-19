import {Subject, Observable, from, of} from 'rxjs';
import {map,switchMap, distinctUntilChanged, catchError} from 'rxjs/operators';
import naturalCompare from 'natural-compare-lite';

import {UIRouter} from '@uirouter/angularjs';
import {DatasourceDto} from 'app/configuration/types';
import ConfigureState from 'app/configuration/services/ConfigureState';
import Datasource from 'app/datasource/services/Datasource';
import AgentManager from 'app/modules/agent/AgentManager.service';
import {IColumnDefOf} from 'ui-grid';
import {dbPresets} from 'app/datasource/dbPresets';

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

    shortClusters$: Observable<Array<DatasourceDto>>;    
    selectedRows$: Subject<Array<DatasourceDto>>;
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
            width: 150
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
            minWidth: 400
        },
        {
            name: 'driverCls',
            displayName: 'driverClass',
            field: 'driverCls',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,           
            width: 180
        },
        {
            name: 'db',
            displayName: 'DB type',
            field: 'db',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,           
            width: 75
        },
        {
            name: 'schemaName',
            displayName: 'Schema',
            field: 'schemaName',
            cellClass: 'ui-grid-number-cell',            
            enableFiltering: false,            
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
        ).subscribe((data)=> {
            this.dataSourceList = data
        }); 
        
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
