

import get from 'lodash/get';
import {Observable, combineLatest, from, of} from 'rxjs';
import {map, tap, pluck, take, filter, catchError, distinctUntilChanged, switchMap, publishReplay, refCount} from 'rxjs/operators';
import ConfigureState from 'app/configuration/services/ConfigureState';
import ConfigSelectors from '../../../configuration/store/selectors';
import {UIRouter} from '@uirouter/angularjs';

export default class PageDatasetsController {
    static $inject = ['$uiRouter', 'ConfigureState','ConfigSelectors'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors
    ) {}

    clusterID$: Observable<string>;
    catalogName$: Observable<string>;
    tooltipsVisible = true;

    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('datasetID'));
        
        let cluster$ = this.clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );  
               

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.catalogName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} DataSets configuration 
            ${isNew ? '' : `‘${get(cluster, 'catalogName','{}')}’`}`;
        });
    }


    $onDestroy() {}
}
