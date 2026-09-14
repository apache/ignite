

import get from 'lodash/get';
import {Observable, combineLatest} from 'rxjs';
import {pluck, switchMap, map} from 'rxjs/operators';
import {default as ConfigureState} from '../../services/ConfigureState';
import {default as ConfigSelectors} from '../../store/selectors';
import {UIRouter} from '@uirouter/angularjs';

export default class PageConfigureController {
    static $inject = ['$uiRouter', 'ConfigureState', 'ConfigSelectors'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigureState: ConfigureState,
        private ConfigSelectors: ConfigSelectors
    ) {}

    clusterID$: Observable<string>;
    clusterName$: Observable<string>;
    tooltipsVisible = true;

    $onInit() {
        this.clusterID$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'));

        const cluster$ = this.clusterID$.pipe(switchMap((id) => this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCluster(id))));

        const isNew$ = this.clusterID$.pipe(map((v) => v === 'new'));

        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} cluster configuration ${isNew ? '' : `‘${get(cluster, 'name')}’`}`;
        });
    }

    $onDestroy() {}
}
