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

import {Observable} from 'rxjs/Observable';
import 'rxjs/add/operator/filter';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/take';
import 'rxjs/add/operator/switchMap';
import 'rxjs/add/operator/merge';
import 'rxjs/add/operator/catch';
import 'rxjs/add/operator/withLatestFrom';
import 'rxjs/add/observable/empty';
import 'rxjs/add/observable/of';
import 'rxjs/add/observable/from';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/observable/timer';
import cloneDeep from 'lodash/cloneDeep';

import {
    ofType
} from '../store/effects';

import {default as ConfigureState} from 'app/components/page-configure/services/ConfigureState';
import {default as ConfigSelectors} from 'app/components/page-configure/store/selectors';

export default class PageConfigure {
    static $inject = ['ConfigureState', 'ConfigSelectors'];

    /**
     * @param {ConfigureState} ConfigureState
     * @param {ConfigSelectors} ConfigSelectors
     */
    constructor(ConfigureState, ConfigSelectors) {
        this.ConfigureState = ConfigureState;
        this.ConfigSelectors = ConfigSelectors;
    }

    getClusterConfiguration({clusterID, isDemo}) {
        return Observable.merge(
            Observable
                .timer(1)
                .take(1)
                .do(() => this.ConfigureState.dispatchAction({type: 'LOAD_COMPLETE_CONFIGURATION', clusterID, isDemo}))
                .ignoreElements(),
            this.ConfigureState.actions$.let(ofType('LOAD_COMPLETE_CONFIGURATION_ERR')).take(1).pluck('error').map((e) => Promise.reject(e)),
            this.ConfigureState.state$
                .let(this.ConfigSelectors.selectCompleteClusterConfiguration({clusterID, isDemo}))
                .filter((c) => c.__isComplete)
                .take(1)
                .map((data) => ({...data, clusters: [cloneDeep(data.cluster)]}))
        )
        .take(1)
        .toPromise();
    }
}
