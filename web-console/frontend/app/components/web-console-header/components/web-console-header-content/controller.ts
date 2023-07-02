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

import {StateService} from '@uirouter/angularjs';
import {UserService, User} from 'app/modules/user/User.service';
import {DemoService} from 'app/modules/demo/Demo.module';
import {tap} from 'rxjs/operators';

export default class WebConsoleHeaderContent {
    static $inject = ['Demo', '$state', 'User'];

    constructor(
        private Demo: DemoService,
        private $state: StateService,
        private User: UserService
    ) {}

    static connectedClusterInvisibleStates = [
        '403', '404', 'signin'
    ];

    user: User;

    _subscriber = this.User.current$.pipe(tap((user) => this.user = user)).subscribe();

    get showConnectedClusters(): boolean {
        return this.user &&
            !this.Demo.enabled &&
            !this.constructor.connectedClusterInvisibleStates.some((state) => this.$state.includes(state)) &&
            !this.user.becomeUsed;
    }

    get showUserMenu(): boolean {
        return !!this.user;
    }

    get showDemoModeButton(): boolean {
        return this.user && !this.user.becomeUsed && !this.Demo.enabled;
    }

    $onDestroy() {
        if (this._subscriber) this._subscriber.unsubscribe();
    }
}
