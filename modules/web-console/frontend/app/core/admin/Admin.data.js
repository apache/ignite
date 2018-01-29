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

export default class IgniteAdminData {
    static $inject = ['$http', 'IgniteMessages', 'IgniteCountries'];

    constructor($http, Messages, Countries) {
        this.$http = $http;
        this.Messages = Messages;
        this.Countries = Countries;
    }

    becomeUser(viewedUserId) {
        return this.$http.get('/api/v1/admin/become', {
            params: {viewedUserId}
        })
        .catch(this.Messages.showError);
    }

    removeUser(user) {
        return this.$http.post('/api/v1/admin/remove', {
            userId: user._id
        })
        .then(() => {
            this.Messages.showInfo(`User has been removed: "${user.userName}"`);
        })
        .catch(({data, status}) => {
            if (status === 503)
                this.Messages.showInfo(data);
            else
                this.Messages.showError('Failed to remove user: ', data);
        });
    }

    toggleAdmin(user) {
        const adminFlag = !user.admin;

        return this.$http.post('/api/v1/admin/toggle', {
            userId: user._id,
            adminFlag
        })
        .then(() => {
            user.admin = adminFlag;

            this.Messages.showInfo(`Admin rights was successfully ${adminFlag ? 'granted' : 'revoked'} for user: "${user.userName}"`);
        })
        .catch((res) => {
            this.Messages.showError(`Failed to ${adminFlag ? 'grant' : 'revok'} admin rights for user: "${user.userName}"`, res);
        });
    }

    prepareUsers(user) {
        const { Countries } = this;

        user.userName = user.firstName + ' ' + user.lastName;
        user.company = user.company ? user.company.toLowerCase() : '';
        user.lastActivity = user.lastActivity || user.lastLogin;
        user.countryCode = Countries.getByName(user.country).code;

        return user;
    }

    loadUsers(params) {
        return this.$http.post('/api/v1/admin/list', params)
            .then(({ data }) => data)
            .then((users) => _.map(users, this.prepareUsers.bind(this)))
            .catch(this.Messages.showError);
    }
}
