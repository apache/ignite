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

// Controller for Admin screen.
export default ['adminController', [
    '$rootScope', '$scope', '$http', '$q', '$state', 'IgniteMessages', 'IgniteConfirm', 'User', 'IgniteCountries',
    ($rootScope, $scope, $http, $q, $state, Messages, Confirm, User, Countries) => {
        $scope.users = null;

        const _reloadUsers = () => {
            $http.post('/api/v1/admin/list')
                .success((users) => {
                    $scope.users = users;

                    _.forEach($scope.users, (user) => {
                        user.userName = user.firstName + ' ' + user.lastName;
                        user.countryCode = Countries.getByName(user.country).code;
                        user.label = user.userName + ' ' + user.email + ' ' +
                            (user.company || '') + ' ' + (user.countryCode || '');
                    });
                })
                .error(Messages.showError);
        };

        _reloadUsers();

        $scope.becomeUser = function(user) {
            $http.get('/api/v1/admin/become', { params: {viewedUserId: user._id}})
                .catch(({data}) => Promise.reject(data))
                .then(User.load)
                .then((becomeUser) => {
                    $rootScope.$broadcast('user', becomeUser);

                    $state.go('base.configuration.clusters');
                })
                .catch(Messages.showError);
        };

        $scope.removeUser = (user) => {
            Confirm.confirm('Are you sure you want to remove user: "' + user.userName + '"?')
                .then(() => {
                    $http.post('/api/v1/admin/remove', {userId: user._id})
                        .success(() => {
                            const i = _.findIndex($scope.users, (u) => u._id === user._id);

                            if (i >= 0)
                                $scope.users.splice(i, 1);

                            Messages.showInfo('User has been removed: "' + user.userName + '"');
                        })
                        .error((err, status) => {
                            if (status === 503)
                                Messages.showInfo(err);
                            else
                                Messages.showError(Messages.errorMessage('Failed to remove user: ', err));
                        });
                });
        };

        $scope.toggleAdmin = (user) => {
            if (user.adminChanging)
                return;

            user.adminChanging = true;

            $http.post('/api/v1/admin/save', {userId: user._id, adminFlag: !user.admin})
                .success(() => {
                    user.admin = !user.admin;

                    Messages.showInfo('Admin right was successfully toggled for user: "' + user.userName + '"');
                })
                .error((err) => {
                    Messages.showError(Messages.errorMessage('Failed to toggle admin right for user: ', err));
                })
                .finally(() => user.adminChanging = false);
        };
    }
]];
