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


import {UIRouter} from '@uirouter/angularjs';
import {ShortCluster} from '../../types';
import {IColumnDefOf} from 'ui-grid';
import {dbPresets} from './dbPresets';
import crudPage from './crud-list.json';

export default class PageDatasourceOverviewController {
    static $inject = [
        '$uiRouter',
        'ConfigureState',
        'DatasourceSelectors'        
    ];
    
    
    

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private DatasourceSelectors: DatasourceSelectors       
    ) {}

    shortClusters$: Observable<Array<ShortCluster>>;    
    selectedRows$: Subject<Array<ShortCluster>>;
    selectedRowsIDs$: Observable<Array<string>>;

    $onDestroy() {
        
    }

    removeClusters(clusters: Array<ShortCluster>) {
        this.ConfigureState.dispatchAction(confirmClustersRemoval(clusters.map((c) => c.id)));

    }

    editCluster(cluster: ShortCluster) {
        return this.$uiRouter.stateService.go('^.edit', {clusterID: cluster.id});
    }

    $onInit() {
        
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
         
        
    }
}
