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

import {NavigationMenu} from '../../types';
import {UIActions, TOGGLE_SIDEBAR, NAVIGATION_MENU_ITEM, HIDE_NAVIGATION_MENU_ITEM, SHOW_NAVIGATION_MENU_ITEM} from '..';

export type UIState = {
    sidebarOpened: boolean,
    navigationMenu: NavigationMenu
};

const defaults: UIState = {
    sidebarOpened: false,
    navigationMenu: []
};

export function uiReducer(state: UIState = defaults, action: UIActions): UIState {
    switch (action.type) {
        case TOGGLE_SIDEBAR:
            return {...state, sidebarOpened: !state.sidebarOpened};
        case NAVIGATION_MENU_ITEM:
            return {...state, navigationMenu: [...state.navigationMenu, action.menuItem]};
        case HIDE_NAVIGATION_MENU_ITEM:
            return {
                ...state,
                navigationMenu: state.navigationMenu.map((i) => i.label === action.label ? {...i, hidden: true} : i)
            };
        case SHOW_NAVIGATION_MENU_ITEM:
            return {
                ...state,
                navigationMenu: state.navigationMenu.map((i) => i.label === action.label ? {...i, hidden: false} : i)
            };
        default:
            return state;
    }
}
