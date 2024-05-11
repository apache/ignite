

import {NavigationMenuItem} from '../../types';

export const TOGGLE_SIDEBAR: 'TOGGLE_SIDEBAR' = 'TOGGLE_SIDEBAR';
export const toggleSidebar = () => ({type: TOGGLE_SIDEBAR});

export const NAVIGATION_MENU_ITEM: 'NAVIGATION_MENU_ITEM' = 'NAVIGATION_MENU_ITEM';
export const navigationMenuItem = (menuItem: NavigationMenuItem) => ({type: NAVIGATION_MENU_ITEM, menuItem});

export const HIDE_NAVIGATION_MENU_ITEM: 'HIDE_NAVIGATION_MENU_ITEM' = 'HIDE_NAVIGATION_MENU_ITEM';
export const hideNavigationMenuItem = (label: NavigationMenuItem['label']) => ({type: HIDE_NAVIGATION_MENU_ITEM, label});

export const SHOW_NAVIGATION_MENU_ITEM: 'SHOW_NAVIGATION_MENU_ITEM' = 'SHOW_NAVIGATION_MENU_ITEM';
export const showNavigationMenuItem = (label: NavigationMenuItem['label']) => ({type: SHOW_NAVIGATION_MENU_ITEM, label});

export type UIActions =
    | ReturnType<typeof toggleSidebar>
    | ReturnType<typeof navigationMenuItem>
    | ReturnType<typeof hideNavigationMenuItem>
    | ReturnType<typeof showNavigationMenuItem>;
