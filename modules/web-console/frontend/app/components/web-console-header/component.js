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

import template from './template.pug';
import './style.scss';

export default {
    template,
    controller: class {
        static $inject = ['$rootScope', '$scope', '$state', 'IgniteBranding', 'UserNotifications'];

        static webAgentDownloadVisibleStates = [
            'base.configuration',
            'base.sql',
            'base.settings'
        ];

        static connectedClustersUnvisibleStates = [
            '403', '404'
        ];

        constructor($rootScope, $scope, $state, branding, UserNotifications) {
            Object.assign(this, {$rootScope, $scope, $state, branding, UserNotifications});
        }

        setWebAgentDownloadVisible() {
            this.isWebAgentDownloadVisible =
                this.constructor.webAgentDownloadVisibleStates.some((state) => this.$state.includes(state));
        }

        setConnectedClustersVisible() {
            this.isConnectedClustersVisible =
                !this.constructor.connectedClustersUnvisibleStates.some((state) => this.$state.includes(state));
        }

        $onInit() {
            this.setWebAgentDownloadVisible();
            this.setConnectedClustersVisible();

            this.$scope.$on('$stateChangeSuccess', () => this.setWebAgentDownloadVisible());
            this.$scope.$on('$stateChangeSuccess', () => this.setConnectedClustersVisible());
        }
    },
    transclude: {
        slotLeft: '?webConsoleHeaderLeft',
        slotRight: '?webConsoleHeaderRight'
    }
};
