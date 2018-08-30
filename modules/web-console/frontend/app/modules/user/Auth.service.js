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

export default ['Auth', ['$http', '$rootScope', '$state', '$window', 'IgniteErrorPopover', 'IgniteMessages', 'gettingStarted', 'User', 'IgniteAgentMonitor',
    ($http, $root, $state, $window, ErrorPopover, Messages, gettingStarted, User, agentMonitor) => {
        return {
            forgotPassword(userInfo) {
                $http.post('/api/v1/password/forgot', userInfo)
                    .then(() => $state.go('password.send'))
                    .catch(({data}) => ErrorPopover.show('forgot_email', Messages.errorMessage(null, data)));
            },
            auth(action, userInfo) {
                $http.post('/api/v1/' + action, userInfo)
                    .then(() => {
                        if (action === 'password/forgot')
                            return;

                        User.read()
                            .then((user) => {
                                $root.$broadcast('user', user);

                                $state.go('base.configuration.clusters');

                                agentMonitor.init();

                                $root.gettingStarted.tryShow();
                            });
                    })
                    .catch((res) => ErrorPopover.show(action + '_email', Messages.errorMessage(null, res)));
            },
            logout() {
                $http.post('/api/v1/logout')
                    .then(() => {
                        User.clean();

                        $window.open($state.href('signin'), '_self');
                    })
                    .catch(Messages.showError);
            }
        };
    }]];
