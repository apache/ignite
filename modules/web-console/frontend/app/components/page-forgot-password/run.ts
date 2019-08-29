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
import {UIRouter} from '@uirouter/angularjs';
import {IIgniteNg1StateDeclaration} from 'app/types';

registerState.$inject = ['$uiRouter', '$translate'];

export function registerState($uiRouter: UIRouter, $translate: ng.translate.ITranslateService) {
    const state: IIgniteNg1StateDeclaration = {
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
            title: $translate.instant('forgotPassword.documentTitle')
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
