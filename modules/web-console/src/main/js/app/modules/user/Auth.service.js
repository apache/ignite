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

export default ['Auth', ['$http', '$rootScope', '$state', '$window', 'IgniteLegacyUtils', 'IgniteMessages', 'gettingStarted', 'User', 'IgniteAgentMonitor',
    ($http, $root, $state, $window, LegacyUtils, Messages, gettingStarted, User, agentMonitor) => {
        let _auth = false;

        try {
            _auth = localStorage.authorized === 'true';
        }
        catch (ignore) {
            // No-op.
        }

        function _authorized(value) {
            try {
                return _auth = localStorage.authorized = !!value;
            } catch (ignore) {
                return _auth = !!value;
            }
        }

        return {
            get authorized() {
                return _auth;
            },
            set authorized(auth) {
                _authorized(auth);
            },
            forgotPassword(userInfo) {
                return $http.post('/api/v1/password/forgot', userInfo)
                    .success(() => $state.go('password.send'))
                    .error((err) => LegacyUtils.showPopoverMessage(null, null, 'forgot_email', Messages.errorMessage(null, err)));
            },
            auth(action, userInfo) {
                return $http.post('/api/v1/' + action, userInfo)
                    .success(() => {
                        return User.read().then((user) => {
                            _authorized(true);

                            $root.$broadcast('user', user);

                            $state.go('base.configuration.clusters');

                            $root.gettingStarted.tryShow();

                            agentMonitor.init();
                        });
                    })
                    .error((err) => LegacyUtils.showPopoverMessage(null, null, action + '_email', Messages.errorMessage(null, err)));
            },
            logout() {
                return $http.post('/api/v1/logout')
                    .then(() => {
                        User.clean();

                        $window.open($state.href('signin'), '_self');
                    })
                    .catch(Messages.showError);
            }
        };
    }]];
