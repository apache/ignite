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

export default class {
    static $inject = ['$modal', '$http', '$state', 'IgniteMessages', '$element'];
    /** @type {JQLite} */
    el;

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IHttpService} $http
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     */
    constructor($modal, $http, $state, Messages, el) {
        this.$http = $http;
        this.$state = $state;
        this.Messages = Messages;
        this.el = el;
    }

    $postLink() {
        this.el.addClass('public-page');
    }

    $onInit() {
        this.$http.post('/api/v1/password/validate/token', {token: this.$state.params.token})
            .then(({data}) => this.ui = data);
    }

    // Try to reset user password for provided token.
    resetPassword() {
        this.$http.post('/api/v1/password/reset', {token: this.ui.token, password: this.ui.password})
            .then(() => {
                this.$state.go('signin');

                this.Messages.showInfo('Password successfully changed');
            })
            .catch(({data, state}) => {
                if (state === 503)
                    this.$state.go('signin');

                this.Messages.showError(data);
            });
    }
}
