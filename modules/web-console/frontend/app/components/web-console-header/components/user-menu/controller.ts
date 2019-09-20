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
