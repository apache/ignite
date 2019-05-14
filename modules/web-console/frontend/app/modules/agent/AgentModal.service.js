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

import templateUrl from 'views/templates/agent-download.tpl.pug';

export default class AgentModal {
    static $inject = ['$rootScope', '$state', '$modal', 'IgniteMessages'];

    constructor($root, $state, $modal, Messages) {
        const self = this;

        this.$root = $root;
        self.$state = $state;
        self.Messages = Messages;

        // Pre-fetch modal dialogs.
        self.modal = $modal({
            templateUrl,
            show: false,
            backdrop: 'static',
            keyboard: false,
            controller: () => self,
            controllerAs: 'ctrl'
        });

        $root.$on('user', (event, user) => self.user = user);
    }

    hide() {
        this.modal.hide();
    }

    /**
     * Close dialog and go by specified link.
     */
    back() {
        this.Messages.hideAlert();

        this.hide();

        _.forEach(angular.element('.modal'), (m) => angular.element(m).scope().$hide());

        if (this.backState)
            this.$state.go(this.backState);
    }

    /**
     * @param {String} backState
     * @param {String} [backText]
     */
    agentDisconnected(backText, backState) {
        const self = this;

        self.backText = backText;
        self.backState = backState;

        self.status = 'agentMissing';

        self.modal.$promise.then(self.modal.show);
    }

    /**
     * @param {String} backState
     * @param {String} [backText]
     */
    clusterDisconnected(backText, backState) {
        const self = this;

        self.backText = backText;
        self.backState = backState;

        self.status = 'nodeMissing';

        self.modal.$promise.then(self.modal.show);
    }

    get securityToken() {
        return this.$root.user.becameToken || this.$root.user.token;
    }
}
