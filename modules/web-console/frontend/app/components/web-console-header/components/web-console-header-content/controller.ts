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

export default class WebConsoleHeaderContent {
    static $inject = ['$rootScope', '$state'];

    constructor(
        private $rootScope: ng.IRootScopeService,
        private $state: StateService
    ) {}

    static connectedClusterInvisibleStates = [
        '403', '404', 'signin'
    ];

    get showConnectedClusters(): boolean {
        return this.$rootScope.user &&
            !this.$rootScope.IgniteDemoMode &&
            !this.constructor.connectedClusterInvisibleStates.some((state) => this.$state.includes(state)) &&
            !this.$rootScope.user.becomeUsed;
    }

    get showUserMenu(): boolean {
        return !!this.$rootScope.user;
    }

    get showDemoModeButton(): boolean {
        return this.$rootScope.user && !this.$rootScope.user.becomeUsed && !this.$rootScope.IgniteDemoMode;
    }
}
