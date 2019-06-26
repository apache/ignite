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
import {tap} from 'rxjs/operators';

export default class UserMenu {
    static $inject = ['User', 'IgniteUserbar', 'AclService', '$state', 'gettingStarted'];

    constructor(
        private User: UserService,
        private IgniteUserbar: any,
        private AclService: any,
        private $state: any,
        private gettingStarted: any
    ) {}

    items = [
        {text: 'Profile', sref: 'base.settings.profile'},
        {text: 'Getting started', click: '$ctrl.gettingStarted.tryShow(true)'}
    ];

    user$ = this.User.current$;

    subscriber = this.user$.pipe(tap(() => {
        this.items.splice(2);

        if (this.AclService.can('admin_page'))
            this.items.push({text: 'Admin panel', sref: 'base.settings.admin'});

        this.items.push(...this.IgniteUserbar);

        if (this.AclService.can('logout'))
            this.items.push({text: 'Log out', sref: 'logout'});
    })).subscribe();

    $onDestroy() {
        if (this.subscriber) this.subscriber.unsubscribe();
    }
}
