/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {default as ConfigSelectors} from '../../../../store/selectors';
import {default as ConfigureState} from '../../../../services/ConfigureState';
import {advancedSaveCluster} from '../../../../store/actionCreators';
import {take, pluck, switchMap, map, filter, distinctUntilChanged, publishReplay, refCount} from 'rxjs/operators';
import {UIRouter} from '@uirouter/angularjs';

// Controller for Clusters screen.
export default class PageConfigureAdvancedCluster {
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
        this.ConfigureState.dispatchAction(advancedSaveCluster(cluster, download));
    }
}
