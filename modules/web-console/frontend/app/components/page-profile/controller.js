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

import _ from 'lodash';

export default class PageProfileController {
    static $inject = [
        '$rootScope', '$scope', '$http', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteFocus', 'IgniteConfirm', 'IgniteCountries', 'User', 'IgniteFormUtils'
    ];

    /**
     * @param {ng.IRootScopeService} $root
     * @param {ng.IScope} $scope
     * @param {ng.IHttpService} $http
     * @param {ReturnType<typeof import('app/services/LegacyUtils.service').default>} LegacyUtils
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     * @param {ReturnType<typeof import('app/services/Focus.service').default>} Focus
     * @param {import('app/services/Confirm.service').Confirm} Confirm
     * @param {ReturnType<typeof import('app/services/Countries.service').default>} Countries
     * @param {ReturnType<typeof import('app/modules/user/User.service').default>} User
     * @param {ReturnType<typeof import('app/services/FormUtils.service').default>} FormUtils
     */
    constructor($root, $scope, $http, LegacyUtils, Messages, Focus, Confirm, Countries, User, FormUtils) {
        this.$root = $root;
        this.$scope = $scope;
        this.$http = $http;
        this.LegacyUtils = LegacyUtils;
        this.Messages = Messages;
        this.Focus = Focus;
        this.Confirm = Confirm;
        this.Countries = Countries;
        this.User = User;
        this.FormUtils = FormUtils;

        this.isLoading = false;
    }

    $onInit() {
        this.ui = {};

        this.User.read()
            .then((user) => this.ui.user = _.cloneDeep(user));

        this.ui.countries = this.Countries.getAll();
    }

    onSecurityTokenPanelClose() {
        this.ui.user.token = this.$root.user.token;
    }

    generateToken() {
        this.Confirm.confirm('Are you sure you want to change security token?<br>If you change the token you will need to restart the agent.')
            .then(() => this.ui.user.token = this.LegacyUtils.randomString(20));
    }

    onPasswordPanelClose() {
        delete this.ui.user.password;
        delete this.ui.user.confirm;
    }

    saveUser() {
        if (this.form.$invalid) {
            this.FormUtils.triggerValidation(this.form);

            return;
        }

        this.isLoading = true;

        return this.$http.post('/api/v1/profile/save', this.ui.user)
            .then(this.User.load)
            .then(() => {
                this.ui.expandedPassword = this.ui.expandedToken = false;

                this.Messages.showInfo('Profile saved.');

                this.Focus.move('profile-username');

                this.$root.$broadcast('user', this.ui.user);
            })
            .catch((res) => this.Messages.showError('Failed to save profile: ', res))
            .finally(() => this.isLoading = false);
    }
}
