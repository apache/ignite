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

import {Store} from '../services/store';

import {UIState, uiReducer} from './reducers/ui';
import {UIActions} from './actions/ui';
import {UIEffects} from './effects/ui';

import {UserActions} from './actions/user';

export * from './actions/ui';
export * from './reducers/ui';
export * from './selectors/ui';

export * from './actions/user';

export {ofType} from './ofType';

export type State = {
    ui: UIState,
};

export type Actions =
    | UserActions
    | UIActions;

export type AppStore = Store<Actions, State>;

register.$inject = ['Store'];
export function register(store: AppStore) {
    store.addReducer('ui', uiReducer);
    store.addEffects(UIEffects);
}
