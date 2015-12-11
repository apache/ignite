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
consoleModule.controller('profileController',
    ['$scope', '$http', '$common', '$focus', '$confirm', function ($scope, $http, $common, $focus, $confirm) {
    $scope.profileUser = angular.copy($scope.$root.user);

    if ($scope.profileUser && !$scope.profileUser.token)
        $scope.profileUser.token = 'No security token. Regenerate please.';

    $scope.generateToken = function () {
        $confirm.confirm('Are you sure you want to change security token?')
            .then(function () {
                $scope.profileUser.token = $commonUtils.randomString(20);
            })
    };

    $scope.profileChanged = function () {
        var old = $scope.$root.user;
        var cur = $scope.profileUser;

        return old && (old.username != cur.username || old.email != cur.email || old.token != cur.token ||
            (cur.changePassword && !$common.isEmptyString(cur.newPassword)));
    };

    $scope.profileCouldBeSaved = function () {
        return $scope.profileForm.$valid && $scope.profileChanged();
    };

    $scope.saveBtnTipText = function () {
        if (!$scope.profileForm.$valid)
            return 'Invalid profile settings';

        return $scope.profileChanged() ? 'Save profile' : 'Nothing to save';
    };

    $scope.saveUser = function () {
        var profile = $scope.profileUser;

        if (profile) {
            var userName = profile.username;
            var changeUsername = userName != $scope.$root.user.username;

            var email = profile.email;
            var changeEmail = email != $scope.$root.user.email;

            var token = profile.token;
            var changeToken = token != $scope.$root.user.token;

            if (changeUsername || changeEmail || changeToken || profile.changePassword) {
                $http.post('/api/v1/profile/save', {
                    _id: profile._id,
                    userName: changeUsername ? userName : undefined,
                    email: changeEmail ? email : undefined,
                    token: changeToken ? token : undefined,
                    newPassword: profile.changePassword ? profile.newPassword : undefined
                }).success(function (user) {
                    $common.showInfo('Profile saved.');

                    profile.changePassword = false;
                    profile.newPassword = null;
                    profile.confirmPassword = null;

                    if (changeUsername)
                        $scope.$root.user.username = userName;

                    if (changeEmail)
                        $scope.$root.user.email = email;

                    $focus('profile-username');
                }).error(function (err) {
                    $common.showError('Failed to save profile: ' + $common.errorMessage(err));
                });
            }
        }
    };
}]);
