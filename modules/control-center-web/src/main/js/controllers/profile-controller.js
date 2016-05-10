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

// Controller for Profile screen.
import consoleModule from 'controllers/common-module';

consoleModule.controller('profileController', [
    '$rootScope', '$scope', '$http', '$common', '$focus', '$confirm', 'IgniteCountries',
    function ($root, $scope, $http, $common, $focus, $confirm, Countries) {
        $scope.user = angular.copy($root.user);

        $scope.countries = Countries.getAll();

        $scope.generateToken = () => {
            $confirm.confirm('Are you sure you want to change security token?')
                .then(() => $scope.user.token = $common.randomString(20))
        };

        const _cleanup = () => {
            const _user = $scope.user;

            if (!$scope.expandedToken)
                _user.token = $root.user.token;

            if (!$scope.expandedPassword) {
                delete _user.password;

                delete _user.confirm;
            }
        };

        const _profileChanged = () => {
            _cleanup();

            const old = $root.user;
            const cur = $scope.user;

            return !_.isEqual(old, cur);
        };

        $scope.profileCouldBeSaved = () => _profileChanged() && $scope.profileForm && $scope.profileForm.$valid;

        $scope.saveBtnTipText = () => {
            if (!_profileChanged())
                return 'Nothing to save';

            return $scope.profileForm && $scope.profileForm.$valid ? 'Save profile' : 'Invalid profile settings';
        };

        $scope.saveUser = () => {
            _cleanup();

            $http.post('/api/v1/profile/save', $scope.user)
                .success(() => {
                    $scope.expandedPassword = false;

                    _cleanup();

                    $scope.expandedToken = false;

                    $root.user = angular.copy($scope.user);

                    $common.showInfo('Profile saved.');

                    $focus('profile-username');
                })
                .error((err) => $common.showError('Failed to save profile: ' + $common.errorMessage(err)));
        };
    }]
);
