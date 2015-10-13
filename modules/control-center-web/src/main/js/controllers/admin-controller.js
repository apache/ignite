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
consoleModule.controller('adminController',
    ['$scope', '$window', '$http', '$common', '$confirm',
    function ($scope, $window, $http, $common, $confirm) {
    $scope.users = null;

    function reload() {
        $http.post('admin/list')
            .success(function (data) {
                $scope.users = data;
            })
            .error(function (errMsg) {
                $common.showError($common.errorMessage(errMsg));
            });
    }

    reload();

    $scope.becomeUser = function (user) {
        $window.location = '/admin/become?viewedUserId=' + user._id;
    };

    $scope.removeUser = function (user) {
        $confirm.confirm('Are you sure you want to remove user: "' + user.username + '"?')
            .then(function () {
                $http.post('admin/remove', {userId: user._id}).success(
                    function () {
                        var i = _.findIndex($scope.users, function (u) {
                            return u._id == user._id;
                        });

                        if (i >= 0)
                            $scope.users.splice(i, 1);

                        $common.showInfo('User has been removed: "' + user.username + '"');
                    }).error(function (errMsg, status) {
                        if (status == 503)
                            $common.showInfo(errMsg);
                        else
                            $common.showError('Failed to remove user: "' + $common.errorMessage(errMsg) + '"');
                    });
            });
    };

    $scope.toggleAdmin = function (user) {
        if (user.adminChanging)
            return;

        user.adminChanging = true;

        $http.post('admin/save', {userId: user._id, adminFlag: user.admin}).success(
            function () {
                $common.showInfo('Admin right was successfully toggled for user: "' + user.username + '"');

                user.adminChanging = false;
            }).error(function (errMsg) {
                $common.showError('Failed to toggle admin right for user: "' + $common.errorMessage(errMsg) + '"');

                user.adminChanging = false;
            });
    }
}]);
