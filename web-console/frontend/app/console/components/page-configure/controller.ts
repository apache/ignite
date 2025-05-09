

import get from 'lodash/get';
import {Observable, combineLatest} from 'rxjs';
import {pluck, switchMap, map} from 'rxjs/operators';
import {default as ConfigureState} from '../../../configuration/services/ConfigureState';
import {default as ConfigSelectors} from '../../../configuration/store/selectors';
import {UIRouter} from '@uirouter/angularjs';

export default class PageConfigureController {
    static $inject = ['$uiRouter', '$location', 'ConfigureState', 'ConfigSelectors'];

    constructor(
        private $uiRouter: UIRouter,
        private $location: ng.ILocationService,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors
    ) {}

    clusterID$: Observable<string>;
    clusterName$: Observable<string>;
    cluster$: Observable<any>;
    tooltipsVisible = true;
    toolButtonVisible = true;
    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));

        const queryParams = this.$location.search();
        if(queryParams && queryParams.simple){  // add query parmas simple=1 for toolButtonVisible=false
            this.toolButtonVisible = false;
        }

        const cluster$ = this.clusterID$.pipe(switchMap((id) => this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCluster(id))));

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} cluster configuration ${isNew ? '' : `‘${get(cluster, 'name')}’`}`;
        });

        this.cluster$ = cluster$
    }

    $onDestroy() {}
}
