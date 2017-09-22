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

// Controller for password reset.
export default ['resetPassword', [
    '$scope', '$modal', '$http', '$state', 'IgniteMessages', 'IgniteFocus',
    ($scope, $modal, $http, $state, Messages, Focus) => {
        if ($state.params.token) {
            $http.post('/api/v1/password/validate/token', {token: $state.params.token})
                .success((res) => {
                    $scope.email = res.email;
                    $scope.token = res.token;
                    $scope.error = res.error;

                    if ($scope.token && !$scope.error)
                        Focus.move('user_password');
                });
        }

        // Try to reset user password for provided token.
        $scope.resetPassword = (reset_info) => {
            $http.post('/api/v1/password/reset', reset_info)
                .success(() => {
                    Messages.showInfo('Password successfully changed');

                    $state.go('base.configuration.clusters');
                })
                .error((err, state) => {
                    Messages.showError(err);

                    if (state === 503)
                        $state.go('base.configuration.clusters');
                });
        };

    }
]];
