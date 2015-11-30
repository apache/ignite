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
 
import angular from 'angular'

angular
.module('ignite-console.Auth', [
	
])
.provider('Auth', function () {
	var _authorized = false;

    try {
        _authorized = localStorage.authorized === 'true';
    } catch (ignore) {
        // No-op.
    }

    function authorized (value) {
        try {
            return _authorized = localStorage.authorized = !!value;
        } catch (ignore) {
            return _authorized = !!value;
        }
    }

    this.$get = ['$http', '$rootScope', '$state', '$common', 'User', function($http, $root, $state, $common, User) {
    	return {
    		get authorized () {
    			return _authorized;
    		},
            auth(action, userInfo) {
                $http.post('/api/v1/' + action, userInfo)
                    .then(User.read)
                    .then(function (user) {
                        if (action != 'password/forgot') {
                            authorized(true);

                            $root.$broadcast('user', user);

                            $state.go('base.configuration.clusters');
                        } else
                            $state.go('password.send');
                    })
                    .catch(function (errMsg) {
                        $common.showPopoverMessage(undefined, undefined, 'user_email', errMsg.data);
                    });
            },
			logout() {
				$http.post('/api/v1/logout')
					.then(function () {
                        User.clean();

                        authorized(false);

						$state.go('login');
					})
					.catch(function (errMsg) {
						$common.showError(errMsg);
					});
			}
    	}
    }]
});
