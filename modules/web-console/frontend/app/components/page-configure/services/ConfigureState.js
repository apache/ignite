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

import {Subject} from 'rxjs/Subject';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import 'rxjs/add/operator/do';
import 'rxjs/add/operator/scan';

export default class ConfigureState {
    constructor() {
        /** @type {Subject<{type: string}>} */
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

        this.actions$.scan(reducer, {}).do((v) => this.state$.next(v)).subscribe();
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
