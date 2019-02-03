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

import publicTemplate from '../../../views/public.pug';

/**
 * @param {import("@uirouter/angularjs").UIRouter} $uiRouter
 */
export function registerState($uiRouter) {
    /** @type {import("app/types").IIgniteNg1StateDeclaration} */
    const state = {
        name: 'forgotPassword',
        url: '/forgot-password',
        views: {
            '': {
                template: publicTemplate
            },
            'page@forgotPassword': {
                component: 'pageForgotPassword'
            }
        },
        unsaved: true,
        tfMetaTags: {
            title: 'Forgot Password'
        },
        resolve: [
            {
                token: 'email',
                deps: ['$uiRouter'],
                /**
                 * @param {import('@uirouter/angularjs').UIRouter} $uiRouter
                 */
                resolveFn($uiRouter) {
                    return $uiRouter.stateService.transition.targetState().params().email;
                }
            }
        ]
    };

    $uiRouter.stateRegistry.register(state);
}

registerState.$inject = ['$uiRouter'];
