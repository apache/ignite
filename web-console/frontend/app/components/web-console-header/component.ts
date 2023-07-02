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

import template from './template.pug';
import './style.scss';
import {AppStore, toggleSidebar} from '../../store';
import {UserService} from '../../modules/user/User.service';
import {map} from 'rxjs/operators';

export default {
    template,
    controller: class {
        static $inject = ['User', '$scope', '$state', 'IgniteBranding', 'UserNotifications', 'Store'];

        constructor(private User: UserService, $scope, $state, branding, UserNotifications, private store: AppStore) {
            Object.assign(this, {$scope, $state, branding, UserNotifications});
        }

        toggleSidebar() {
            this.store.dispatch(toggleSidebar());
        }

        userIsAuthorized$ = this.User.current$.pipe(map((user) => !!user))
    },
    transclude: true,
    bindings: {
        hideMenuButton: '<?'
    }
};
