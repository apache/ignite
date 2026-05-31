

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
