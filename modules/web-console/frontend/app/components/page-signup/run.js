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

import publicTemplate from '../../../views/public.pug';

/**
 * @param {import("@uirouter/angularjs").UIRouter} $uiRouter
 */
export function registerState($uiRouter) {
    /** @type {import("app/types").IIgniteNg1StateDeclaration} */
    const state = {
        name: 'signup',
        url: '/signup',
        views: {
            '': {
                template: publicTemplate
            },
            'page@signup': {
                component: 'pageSignup'
            }
        },
        unsaved: true,
        redirectTo: (trans) => {
            const skipStates = new Set(['signin', 'forgotPassword', 'landing']);

            if (skipStates.has(trans.from().name))
                return;

            return trans.injector().get('User').read()
                .then(() => {
                    try {
                        const {name, params} = JSON.parse(localStorage.getItem('lastStateChangeSuccess'));

                        const restored = trans.router.stateService.target(name, params);

                        return restored.valid() ? restored : 'default-state';
                    }
                    catch (ignored) {
                        return 'default-state';
                    }
                })
                .catch(() => true);
        },
        tfMetaTags: {
            title: 'Sign Up'
        }
    };
    $uiRouter.stateRegistry.register(state);
}

registerState.$inject = ['$uiRouter'];
