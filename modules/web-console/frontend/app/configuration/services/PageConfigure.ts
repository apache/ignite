/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import cloneDeep from 'lodash/cloneDeep';

import {merge, timer} from 'rxjs';
import {take, tap, ignoreElements, filter, map, pluck} from 'rxjs/operators';

import {
    ofType
} from '../store/effects';

import {default as ConfigureState} from './ConfigureState';
import {default as ConfigSelectors} from '../store/selectors';

export default class PageConfigure {
    static $inject = ['ConfigureState', 'ConfigSelectors'];

    constructor(private ConfigureState: ConfigureState, private ConfigSelectors: ConfigSelectors) {}

    getClusterConfiguration({clusterID, isDemo}: {clusterID: string, isDemo: boolean}) {
        return merge(
            timer(1).pipe(
                take(1),
                tap(() => this.ConfigureState.dispatchAction({type: 'LOAD_COMPLETE_CONFIGURATION', clusterID, isDemo})),
                ignoreElements()
            ),
            this.ConfigureState.actions$.pipe(
                ofType('LOAD_COMPLETE_CONFIGURATION_ERR'),
                take(1),
                pluck('error'),
                map((e) => Promise.reject(e))
            ),
            this.ConfigureState.state$.pipe(
                this.ConfigSelectors.selectCompleteClusterConfiguration({clusterID, isDemo}),
                filter((c) => c.__isComplete),
                take(1),
                map((data) => ({...data, clusters: [cloneDeep(data.cluster)]}))
            )
        ).pipe(take(1))
        .toPromise();
    }
}
