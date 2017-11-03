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
import 'rxjs/add/operator/scan';
import 'rxjs/add/operator/share';
import 'rxjs/add/operator/publishReplay';
import {reducer as listReducer} from '../reducer';
import {reducer as configureBasicReducer} from '../../page-configure-basic/reducer';

let devTools;
const actions$ = new Subject();

const replacer = (key, value) => {
    if (value instanceof Map) {
        return {
            data: [...value],
            __serializedType__: 'Map'
        };
    }
    if (value instanceof Symbol) {
        return {
            data: String(value),
            __serializedType__: 'Symbol'
        };
    }
    return value;
};

const reviver = (key, value) => {
    if (typeof value === 'object' && value !== null && '__serializedType__' in value) {
        const data = value.data;
        switch (value.__serializedType__) {
            case 'Map':
                return new Map(value.data);
            default:
                return data;
        }
    }
    return value;
};

if (window.__REDUX_DEVTOOLS_EXTENSION__) {
    devTools = window.__REDUX_DEVTOOLS_EXTENSION__.connect({
        name: 'Ignite configuration',
        serialize: {
            replacer,
            reviver
        }
    });
    devTools.subscribe((e) => {
        if (e.type === 'DISPATCH' && e.state) actions$.next(e);
    });
}

const reducer = (state = {}, action) => {
    if (action.type === 'DISPATCH') return JSON.parse(action.state, reviver);

    const value = {
        list: listReducer(state.list, action),
        configureBasic: configureBasicReducer(state.configureBasic, action, state)
    };

    devTools && devTools.send(action, value);

    return value;
};

const state$ = actions$.scan(reducer, void 0).publishReplay(1).refCount();

state$.subscribe();

export default class ConfigureState {
    state$ = state$;
    dispatchAction(action) {
        actions$.next(action);
    }
}
