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

import _ from 'lodash';

export default class IgniteAdminData {
    static $inject = ['$http', 'IgniteMessages', 'IgniteCountries'];

    /**
     * @param {ng.IHttpService} $http
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     * @param {ReturnType<typeof import('app/services/Countries.service').default>} Countries
     */
    constructor($http, Messages, Countries) {
        this.$http = $http;
        this.Messages = Messages;
        this.Countries = Countries;
    }

    /**
     * @param user User to become.
     */
    becomeUser(user) {
        return this.$http
            .post('/api/v1/admin/become', {id: user.id})
            .catch(this.Messages.showError);
    }

    /**
     * @param user User to remove.
     */
    removeUser(user) {
        return this.$http
            .delete(`/api/v1/admin/users/${user.id}`)
            .then(() => this.Messages.showInfo(`User has been removed: "${user.userName}"`))
            .catch(({data, status}) => {
                if (status === 503)
                    this.Messages.showInfo(data);
                else
                    this.Messages.showError('Failed to remove user: ', data);
            });
    }

    /**
     * @param user User to toggle admin role.
     */
    toggleAdmin(user) {
        const admin = !user.admin;

        return this.$http
            .post('/api/v1/admin/toggle', {id: user.id, admin})
            .then(() => {
                user.admin = admin;

                this.Messages.showInfo(`Admin rights was successfully ${admin ? 'granted' : 'revoked'} for user: "${user.userName}"`);
            })
            .catch((res) => {
                this.Messages.showError(`Failed to ${admin ? 'grant' : 'revoke'} admin rights for user: "${user.userName}". <br/>`, res);
            });
    }


    /**
     * @param {import('app/modules/user/User.service').User} user
     */
    prepareUsers(user) {
        const { Countries } = this;

        user.userName = user.firstName + ' ' + user.lastName;
        user.company = user.company ? user.company.toLowerCase() : '';
        user.lastActivity = user.lastActivity || user.lastLogin;
        user.countryCode = Countries.getByName(user.country).code;

        return user;
    }

    /**
     * Load users.
     *
     * @param params Dates range.
     * @return {*}
     */
    loadUsers(params) {
        return this.$http.post('/api/v1/admin/list', params)
            .then(({ data }) => data)
            .then((users) => _.map(users, this.prepareUsers.bind(this)))
            .catch(this.Messages.showError);
    }

    /**
     * @param userInfo
     */
    registerUser(userInfo) {
        return this.$http.put('/api/v1/admin/users', userInfo)
            .then(({ data }) => data)
            .catch(this.Messages.showError);
    }
}
