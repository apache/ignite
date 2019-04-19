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

import {State, AppStore} from '..';
import {pluck, map} from 'rxjs/operators';
import {pipe} from 'rxjs';
import {memoize, orderBy} from 'lodash';

const orderMenu = <T extends {order: number}>(menu: Array<T>) => orderBy(menu, 'order');

export const selectSidebarOpened = () => pluck<State, State['ui']['sidebarOpened']>('ui', 'sidebarOpened');
export const selectNavigationMenu = () => pipe(
    pluck<State, State['ui']['navigationMenu']>('ui', 'navigationMenu'),
    map(orderMenu)
);
