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
