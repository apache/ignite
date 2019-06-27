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
import {default as MessagesFactory} from 'app/services/Messages.service';
import {default as CountriesFactory} from 'app/services/Countries.service';
import {User, UserService} from 'app/modules/user/User.service';
import {UIRouter} from '@uirouter/angularjs';
import {default as NotebookData} from 'app/components/page-queries/notebook.data';

export default class IgniteAdminData {
    static $inject = ['$http', 'IgniteMessages', 'IgniteCountries', 'User', '$uiRouter', 'IgniteNotebookData'];

    constructor(
        private $http: ng.IHttpService,
        private Messages: ReturnType<typeof MessagesFactory>,
        private Countries: ReturnType<typeof CountriesFactory>,
        private UserService: UserService,
        private uiRouter: UIRouter,
        private Notebook: NotebookData
    ) {}

    /**
     * @param email User email.
     */
    becomeUser(email: string) {
        return this.$http.get('/api/v1/admin/login/impersonate', {
            params: {email}
        })
            .catch(this.Messages.showError);
    }

    revertIdentity() {
        this.$http.get('/api/v1/logout/impersonate')
            .then(() => this.UserService.load())
            .then(() => this.uiRouter.stateService.go('base.settings.admin'))
            // TODO GG-19514: separate side effect from main action.
            .then(() => this.Notebook.load())
            .catch(this.Messages.showError);
    }

    removeUser(user: User) {
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

    toggleAdmin(user: User) {
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

    prepareUsers(user: User) {
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
            .catch(({data}) => {throw data;});
    }
}
