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

// Sign in controller.
// TODO IGNITE-1936 Refactor this controller.
export default ['auth', [
    '$scope', 'IgniteFocus', 'IgniteCountries', 'Auth',
    ($scope, Focus, Countries, Auth) => {
        $scope.auth = Auth.auth;
        $scope.forgotPassword = Auth.forgotPassword;
        $scope.action = 'signin';
        $scope.countries = Countries.getAll();

        Focus.move('user_email');
    }
]];
