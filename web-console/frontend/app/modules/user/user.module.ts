/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import {default as UserServiceFactory, UserService} from './User.service';
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

run.$inject = ['$transitions', 'AclService', 'User', 'IgniteActivitiesData'];

/**
 * @param {import('@uirouter/angularjs').TransitionService} $transitions
 * @param {unknown} AclService
 * @param {ReturnType<typeof import('app/components/activities-user-dialog/index').default>} Activities
 */
function run($transitions, AclService, User: UserService, Activities) {
    AclService.setAbilities(aclData);
    AclService.attachRole('guest');

    User.current$.subscribe((user) => {
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
            .catch((err) => {
                return $state.target(trans.to().failState || (err.status === 503 ? 'base.503' : 'base.403'));
            });
    });
}

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
    .service('User', UserServiceFactory)
    .run(run);
