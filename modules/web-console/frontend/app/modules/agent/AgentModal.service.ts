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

import angular from 'angular';
import _ from 'lodash';
import templateUrl from 'views/templates/agent-download.tpl.pug';
import {User, UserService} from 'app/modules/user/User.service';
import {tap} from 'rxjs/operators';

export default class AgentModal {
    static $inject = ['User', '$state', '$modal', 'IgniteMessages'];

    /**
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     */
    constructor(private User: UserService, $state, $modal, Messages) {
        const self = this;

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

        User.current$.pipe(tap((user) => this.user = user)).subscribe();
    }

    user: User;

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
        return this.user.becameToken || this.user.token;
    }
}
