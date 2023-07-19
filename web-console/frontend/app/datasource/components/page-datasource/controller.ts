

import get from 'lodash/get';
import {Observable, combineLatest, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import Datasource from 'app/datasource/services/Datasource';
import {UIRouter} from '@uirouter/angularjs';

export default class PageDatasourceController {
    static $inject = ['$uiRouter', 'ConfigureState', 'Datasource'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private Datasource: Datasource
        
    ) {}

    clusterID$: Observable<string>;
    clusterName$: Observable<string>;
    tooltipsVisible = true;

    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));

        
        let cluster$ = this.clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return from(this.Datasource.selectDatasource(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
               

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} DataSource configuration 
            ${isNew ? '' : `‘${get(cluster, 'jndiName','')}’`}`;
        });
    }

    $onDestroy() {}
}
