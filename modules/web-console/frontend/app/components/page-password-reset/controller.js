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

export default class {
    static $inject = ['$modal', '$http', '$state', 'IgniteMessages'];

    /**
     * @param {mgcrea.ngStrap.modal.IModalService} $modal
     * @param {ng.IHttpService} $http
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     */
    constructor($modal, $http, $state, Messages) {
        this.$http = $http;
        this.$state = $state;
        this.Messages = Messages;
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
