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
