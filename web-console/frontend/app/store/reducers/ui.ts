

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
