/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import angular from 'angular';
import aclData from './permissions';

import Auth from './Auth.service';
import User from './User.service';
import {registerInterceptor} from './emailConfirmationInterceptor';

/**
 * @param {ng.auto.IInjectorService} $injector
 * @param {ng.IQService} $q
 */
function sessionRecoverer($injector, $q) {
    /** @type {ng.IHttpInterceptor} */
    return {
        responseError: (response) => {
            // Session has expired
            if (response.status === 401) {
                $injector.get('User').clean();

                const stateName = $injector.get('$uiRouterGlobals').current.name;

                if (!_.includes(['', 'signin', 'terms', '403', '404'], stateName))
                    $injector.get('$state').go('signin');
            }

            return $q.reject(response);
        }
    };
}

sessionRecoverer.$inject = ['$injector', '$q'];

/**
 * @param {ng.IRootScopeService} $root
 * @param {import('@uirouter/angularjs').TransitionService} $transitions
 * @param {unknown} AclService
 * @param {ReturnType<typeof import('./User.service').default>} User
 * @param {ReturnType<typeof import('app/components/activities-user-dialog/index').default>} Activities
 */
function run($root, $transitions, AclService, User, Activities) {
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
        const {permission} = trans.to();

        if (_.isEmpty(permission))
            return;

        return trans.injector().get('User').read()
            .then(() => {
                if (!AclService.can(permission))
                    throw new Error('Illegal access error');
            })
            .catch(() => {
                return $state.target(trans.to().failState || 'base.403');
            });
    });
}

run.$inject = ['$rootScope', '$transitions', 'AclService', 'User', 'IgniteActivitiesData'];

angular
    .module('ignite-console.user', [
        'mm.acl',
        'ignite-console.config',
        'ignite-console.core'
    ])
    .factory('sessionRecoverer', sessionRecoverer)
    .config(registerInterceptor)
    .config(['$httpProvider', ($httpProvider) => {
        $httpProvider.interceptors.push('sessionRecoverer');
    }])
    .service('Auth', Auth)
    .service('User', User)
    .run(run);
