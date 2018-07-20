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

import _ from 'lodash';

export default class PageProfileController {
    static $inject = [
        '$rootScope', '$scope', '$http', 'IgniteLegacyUtils', 'IgniteMessages', 'IgniteFocus', 'IgniteConfirm', 'IgniteCountries', 'User'
    ];

    constructor($root, $scope, $http, LegacyUtils, Messages, Focus, Confirm, Countries, User) {
        Object.assign(this, {$root, $scope, $http, LegacyUtils, Messages, Focus, Confirm, Countries, User});
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
        return this.$http.post('/api/v1/profile/save', this.ui.user)
            .then(this.User.load)
            .then(() => {
                this.ui.expandedPassword = this.ui.expandedToken = false;

                this.Messages.showInfo('Profile saved.');

                this.Focus.move('profile-username');

                this.$root.$broadcast('user', this.ui.user);
            })
            .catch((res) => this.Messages.showError('Failed to save profile: ', res));
    }
}
