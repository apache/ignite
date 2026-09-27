

import {State} from '..';
import {pluck, map} from 'rxjs/operators';
import {pipe} from 'rxjs';
import {orderBy} from 'lodash';

const orderMenu = <T extends {order: number}>(menu: Array<T>) => orderBy(menu, 'order');

export const selectSidebarOpened = () => pluck<State, State['ui']['sidebarOpened']>('ui', 'sidebarOpened');
export const selectNavigationMenu = () => pipe(
    pluck<State, State['ui']['navigationMenu']>('ui', 'navigationMenu'),
    map(orderMenu)
);
