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
consoleModule.controller('profileController', [
    '$rootScope', '$scope', '$http', '$common', '$focus', '$confirm', 'IgniteCountries',
    function ($rootScope, $scope, $http, $common, $focus, $confirm, countries) {
        $scope.user = angular.copy($scope.$root.user);

        $scope.countries = countries;

        if ($scope.user && !$scope.user.token)
            $scope.user.token = 'No security token. Regenerate please.';

        $scope.generateToken = function () {
            $confirm.confirm('Are you sure you want to change security token?')
                .then(function () {
                    $scope.user.token = $commonUtils.randomString(20);
                })
        };

        function _profileChanged() {
            var old = $rootScope.user;
            var cur = $scope.user;

            return !_.isEqual(old, cur) || ($scope.expandedPassword && !$common.isEmptyString($scope.newPassword));
        }

        $scope.profileCouldBeSaved = function () {
            return _profileChanged() && $scope.profileForm && $scope.profileForm.$valid;
        };

        $scope.saveBtnTipText = function () {
            if (!_profileChanged())
                return 'Nothing to save';

            return $scope.profileForm && $scope.profileForm.$valid ? 'Save profile' : 'Invalid profile settings';
        };

        $scope.saveUser = function () {
            var _user = angular.copy($scope.user);

            if ($scope.expandedPassword)
                _user.password = $scope.newPassword;

            $http.post('/api/v1/profile/save', _user)
                .success(function () {
                    $scope.expandedToken = false;

                    $scope.expandedToken = false;
                    $scope.newPassword = '';
                    $scope.confirmPassword = '';

                    $rootScope.user = angular.copy($scope.user);

                    $common.showInfo('Profile saved.');

                    $focus('profile-username');
                })
                .error(function (err) {
                    $common.showError('Failed to save profile: ' + $common.errorMessage(err));
                });
        };
    }]
);
