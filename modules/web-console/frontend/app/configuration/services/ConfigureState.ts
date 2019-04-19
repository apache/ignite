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

import {Subject, BehaviorSubject} from 'rxjs';
import {tap, scan} from 'rxjs/operators';

export default class ConfigureState {
    actions$: Subject<{type: string}>;

    constructor() {
        this.actions$ = new Subject();
        this.state$ = new BehaviorSubject({});
        this._combinedReducer = (state, action) => state;

        const reducer = (state = {}, action) => {
            try {
                return this._combinedReducer(state, action);
            }
            catch (e) {
                console.error(e);

                return state;
            }
        };

        this.actions$.pipe(
            scan(reducer, {}),
            tap((v) => this.state$.next(v))
        ).subscribe();
    }

    addReducer(combineFn) {
        const old = this._combinedReducer;

        this._combinedReducer = (state, action) => combineFn(old(state, action), action);

        return this;
    }

    dispatchAction(action) {
        if (typeof action === 'function')
            return action((a) => this.actions$.next(a), () => this.state$.getValue());

        this.actions$.next(action);

        return action;
    }
}
