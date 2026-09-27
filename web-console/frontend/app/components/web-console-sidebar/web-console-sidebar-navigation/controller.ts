

import {AppStore, selectNavigationMenu} from '../../../store';

export default class WebConsoleSidebarNavigation {
    static $inject = ['Store'];

    constructor(private store: AppStore) {}

    menu$ = this.store.state$.pipe(selectNavigationMenu());
}
