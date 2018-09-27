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

/**
 * @typedef User
 * @prop {string} _id
 * @prop {boolean} admin
 * @prop {string} country
 * @prop {string} email
 * @prop {string} firstName
 * @prop {string} lastName
 * @prop {string} lastActivity
 * @prop {string} lastLogin
 * @prop {string} registered
 * @prop {string} token
 */

/**
 * @param {ng.IQService} $q
 * @param {ng.auto.IInjectorService} $injector
 * @param {ng.IRootScopeService} $root
 * @param {import('@uirouter/angularjs').StateService} $state
 * @param {ng.IHttpService} $http
 */
export default function User($q, $injector, $root, $state, $http) {
    /** @type {ng.IPromise<User>} */
    let user;

    return {
        /**
         * @returns {ng.IPromise<User>}
         */
        load() {
            return user = $http.post('/api/v1/user')
                .then(({data}) => {
                    $root.user = data;

                    $root.$broadcast('user', $root.user);

                    return $root.user;
                })
                .catch(({data}) => {
                    user = null;

                    return $q.reject(data);
                });
        },
        read() {
            if (user)
                return user;

            return this.load();
        },
        clean() {
            delete $root.user;

            delete $root.IgniteDemoMode;

            sessionStorage.removeItem('IgniteDemoMode');
        }
    };
}

User.$inject = ['$q', '$injector', '$rootScope', '$state', '$http'];
