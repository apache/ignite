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

import angular from 'angular';
import aclData from './permissions';

import Auth from './Auth.service';
import User from './User.service';

angular.module('ignite-console.user', [
    'mm.acl',
    'ignite-console.config',
    'ignite-console.core'
])
.factory('sessionRecoverer', ['$injector', '$q', function($injector, $q) {
    return {
        responseError: (response) => {
            // Session has expired
            if (response.status === 401) {
                $injector.get('User').clean();

                const stateName = $injector.get('$uiRouterGlobals').current.name;

                if (!_.includes(['', 'signin'], stateName))
                    $injector.get('$state').go('signin');
            }

            return $q.reject(response);
        }
    };
}])
.config(['$httpProvider', ($httpProvider) => {
    $httpProvider.interceptors.push('sessionRecoverer');
}])
.service('Auth', Auth)
.service(...User)
.run(['$rootScope', '$transitions', 'AclService', 'User', 'IgniteActivitiesData', ($root, $transitions, AclService, User, Activities) => {
    AclService.setAbilities(aclData);
    AclService.attachRole('guest');

    $root.$on('user', (event, user) => {
        if (!user)
            return;

        AclService.flushRoles();

        let role = 'user';

        if (user.admin)
            role = 'admin';

        if (user.becomeUsed)
            role = 'becomed';

        AclService.attachRole(role);
    });

    $transitions.onBefore({}, (trans) => {
        const $state = trans.router.stateService;
        const {name, permission} = trans.to();

        if (_.isEmpty(permission))
            return;

        return trans.injector().get('User').read()
            .then(() => {
                if (AclService.can(permission)) {
                    Activities.post({action: $state.href(name, trans.params('to'))});

                    return;
                }

                return $state.target(trans.to().failState || '403');
            })
            .catch(() => {
                return $state.target(trans.to().failState || '403');
            });
    });
}]);
