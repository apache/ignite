

import {AppStore, selectSidebarOpened} from '../../store';
import {UserService} from 'app/modules/user/User.service';
import {map} from 'rxjs/operators';

export default class WebConsoleSidebar {
    static $inject = ['User', 'Store'];

    constructor(
        private User: UserService,
        private store: AppStore
    ) {}

    sidebarOpened$ = this.store.state$.pipe(selectSidebarOpened());
    showNavigation$ = this.User.current$.pipe(map((user) => !!user))
}
