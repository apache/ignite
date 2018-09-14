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

import get from 'lodash/get';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/merge';
import {combineLatest} from 'rxjs/observable/combineLatest';
import 'rxjs/add/operator/distinctUntilChanged';
import {default as ConfigureState} from './services/ConfigureState';
import {default as ConfigSelectors} from './store/selectors';

export default class PageConfigureController {
    static $inject = ['$uiRouter', 'ConfigureState', 'ConfigSelectors'];

    /**
     * @param {uirouter.UIRouter} $uiRouter
     * @param {ConfigureState} ConfigureState
     * @param {ConfigSelectors} ConfigSelectors
     */
    constructor($uiRouter, ConfigureState, ConfigSelectors) {
        this.$uiRouter = $uiRouter;
        this.ConfigureState = ConfigureState;
        this.ConfigSelectors = ConfigSelectors;
    }

    $onInit() {
        /** @type {Observable<string>} */
        this.clusterID$ = this.$uiRouter.globals.params$.pluck('clusterID');
        const cluster$ = this.clusterID$.switchMap((id) => this.ConfigureState.state$.let(this.ConfigSelectors.selectCluster(id)));
        const isNew$ = this.clusterID$.map((v) => v === 'new');
        this.clusterName$ = combineLatest(cluster$, isNew$, (cluster, isNew) => {
            return `${isNew ? 'Create' : 'Edit'} cluster configuration ${isNew ? '' : `‘${get(cluster, 'name')}’`}`;
        });

        this.tooltipsVisible = true;
    }

    $onDestroy() {}
}
