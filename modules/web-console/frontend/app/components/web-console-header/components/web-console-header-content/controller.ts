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
