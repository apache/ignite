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

import {AppStore, USER, ofType, hideNavigationMenuItem, showNavigationMenuItem} from '..';
import {map} from 'rxjs/operators';

export class UIEffects {
    static $inject = ['Store'];
    constructor(private store: AppStore) {}

    toggleQueriesNavItemEffect$ = this.store.actions$.pipe(
        ofType(USER),
        map((action) => {
            const QUERY_LABEL = 'Queries';
            return action.user.becomeUsed ? hideNavigationMenuItem(QUERY_LABEL) : showNavigationMenuItem(QUERY_LABEL);
        })
    )
}
