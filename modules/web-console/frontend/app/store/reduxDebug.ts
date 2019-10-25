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

import {reducer, devTools} from './reduxDevtoolsIntegration';
import {AppStore} from '.';
import {filter, withLatestFrom, tap} from 'rxjs/operators';

run.$inject = ['Store'];

export function run(store: AppStore) {
    if (devTools) {
        devTools.subscribe((e) => {
            if (e.type === 'DISPATCH' && e.state) store.dispatch(e);
        });

        const ignoredActions = new Set([
        ]);

        store.actions$.pipe(
            filter((e) => e.type !== 'DISPATCH'),
            withLatestFrom(store.state$.skip(1)),
            tap(([action, state]) => {
                if (ignoredActions.has(action.type)) return;
                devTools.send(action, state);
                console.log(action);
            })
        ).subscribe();

        store.addReducer(reducer);
    }
}
