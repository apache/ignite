

import {UserService} from 'app/modules/user/User.service';
import {map} from 'rxjs/operators';

export default class UserMenu {
    static $inject = ['User', 'AclService', '$state'];

    constructor(
        private User: UserService,
        private AclService: any,
        private $state: any
    ) {}

    user$ = this.User.current$;

    menu$ = this.user$.pipe(map(() => {
        return [
            {text: 'Profile', sref: 'base.settings.profile'},
            this.AclService.can('admin_page') ? {text: 'Admin panel', sref: 'base.settings.admin'} : null,
            this.AclService.can('logout') ? {text: 'Log out', sref: 'logout'} : null
        ].filter((v) => !!v);
    }));
}
