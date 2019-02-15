/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import angular from 'angular';
import _ from 'lodash';
import templateUrl from 'views/templates/agent-download.tpl.pug';

export default class AgentModal {
    static $inject = ['$rootScope', '$state', '$modal', 'IgniteMessages'];

    /**
     * @param {ng.IRootScopeService} $root
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     */
    constructor($root, $state, $modal, Messages) {
        const self = this;

        this.$root = $root;
        this.$state = $state;
        this.Messages = Messages;

        // Pre-fetch modal dialogs.
        this.modal = $modal({
            templateUrl,
            show: false,
            backdrop: 'static',
            keyboard: false,
            controller() { return self;},
            controllerAs: 'ctrl'
        });

        $root.$on('user', (event, user) => this.user = user);
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
        this.backText = backText;
        this.backState = backState;

        this.status = 'agentMissing';

        this.modal.$promise.then(() => this.modal.show());
    }

    /**
     * @param {String} backState
     * @param {String} [backText]
     */
    clusterDisconnected(backText, backState) {
        this.backText = backText;
        this.backState = backState;

        this.status = 'nodeMissing';

        this.modal.$promise.then(() => this.modal.show());
    }

    get securityToken() {
        return this.$root.user.becameToken || this.$root.user.token;
    }
}
