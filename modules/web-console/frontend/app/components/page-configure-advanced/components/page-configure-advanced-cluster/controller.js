/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {default as ConfigSelectors} from 'app/components/page-configure/store/selectors';
import {default as ConfigureState} from 'app/components/page-configure/services/ConfigureState';
import {advancedSaveCluster} from 'app/components/page-configure/store/actionCreators';
import {take, pluck, switchMap, map, filter, distinctUntilChanged, publishReplay, refCount} from 'rxjs/operators';

// Controller for Clusters screen.
export default class PageConfigureAdvancedCluster {
    static $inject = ['$uiRouter', 'ConfigSelectors', 'ConfigureState'];

    /**
     * @param {uirouter.UIRouter} $uiRouter
     * @param {ConfigSelectors} ConfigSelectors
     * @param {ConfigureState} ConfigureState
     */
    constructor($uiRouter, ConfigSelectors, ConfigureState) {
        this.$uiRouter = $uiRouter;
        this.ConfigSelectors = ConfigSelectors;
        this.ConfigureState = ConfigureState;
    }

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
