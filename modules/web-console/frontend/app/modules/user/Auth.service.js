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

/**
 * @typedef {object} SignupUserInfo
 * @prop {string} email
 * @prop {string} password
 * @prop {string} firstName
 * @prop {string} lastName
 * @prop {string} company
 * @prop {string} country
 */

export default class AuthService {
    static $inject = ['$http', '$rootScope', '$state', '$window', 'IgniteMessages', 'gettingStarted', 'User'];
    /**
     * @param {ng.IHttpService} $http
     * @param {ng.IRootScopeService} $root
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {ng.IWindowService} $window
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     * @param {ReturnType<typeof import('app/modules/getting-started/GettingStarted.provider').service>} gettingStarted
     * @param {ReturnType<typeof import('./User.service').default>} User
     */
    constructor($http, $root, $state, $window, Messages, gettingStarted, User) {
        this.$http = $http;
        this.$root = $root;
        this.$state = $state;
        this.$window = $window;
        this.Messages = Messages;
        this.gettingStarted = gettingStarted;
        this.User = User;
    }
    /**
     * @param {SignupUserInfo} userInfo
     */
    signnup(userInfo) {
        return this._auth('signup', userInfo);
    }
    /**
     * @param {string} email
     * @param {string} password
     */
    signin(email, password) {
        return this._auth('signin', {email, password});
    }
    /**
     * @param {string} email
     */
    remindPassword(email) {
        return this._auth('password/forgot', {email}).then(() => this.$state.go('password.send'));
    }

    // TODO IGNITE-7994: Remove _auth and move API calls to corresponding methods
    /**
     * Performs the REST API call.
     * @private
     * @param {('signin'|'signup'|'password/forgot')} action
     * @param {{email:string,password:string}|SignupUserInfo|{email:string}} userInfo
     */
    _auth(action, userInfo) {
        return this.$http.post('/api/v1/' + action, userInfo)
            .then(() => {
                if (action === 'password/forgot')
                    return;

                this.User.read()
                    .then((user) => {
                        this.$root.$broadcast('user', user);

                        this.$state.go('default-state');

                        this.$root.gettingStarted.tryShow();
                    });
            });
    }
    logout() {
        return this.$http.post('/api/v1/logout')
            .then(() => {
                this.User.clean();

                this.$window.open(this.$state.href('signin'), '_self');
            })
            .catch((e) => this.Messages.showError(e));
    }
}
