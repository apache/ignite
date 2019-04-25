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
