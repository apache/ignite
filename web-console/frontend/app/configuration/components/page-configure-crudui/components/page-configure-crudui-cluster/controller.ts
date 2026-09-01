

import {default as ConfigSelectors} from '../../../../store/selectors';
import {default as ConfigureState} from '../../../../services/ConfigureState';
import {advancedSaveCluster} from '../../../../store/actionCreators';
import {take, pluck, switchMap, map, filter, distinctUntilChanged, publishReplay, refCount} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angularjs';

// Controller for Clusters screen.
export default class PageConfigureCrudUICluster {
    static $inject = ['$uiRouter', 'ConfigSelectors', 'ConfigureState'];

    constructor(
        private $uiRouter: UIRouter,
        private ConfigSelectors: ConfigSelectors,
        private ConfigureState: ConfigureState
    ) {}

    $onInit() {
        const clusterID$ = this.$uiRouter.globals.params$.pipe(
            take(1),
            pluck('clusterID'),
            filter((v) => v),
            take(1)
        );

        this.shortCaches$ = this.ConfigureState.state$.pipe(this.ConfigSelectors.selectCurrentShortCaches);

        this.originalCluster$ = clusterID$.pipe(
            distinctUntilChanged(),
            switchMap((id) => {
                return this.ConfigureState.state$.pipe(this.ConfigSelectors.selectClusterToEdit(id));
            }),
            distinctUntilChanged(),
            publishReplay(1),
            refCount()
        );

        this.isNew$ = this.$uiRouter.globals.params$.pipe(pluck('clusterID'), map((id) => id === 'new'));

        this.isBlocked$ = clusterID$;
    }

    save({cluster, download}) {
        cluster.crudui = {enabled: true};
        this.ConfigureState.dispatchAction(advancedSaveCluster(cluster, download));
    }
}
