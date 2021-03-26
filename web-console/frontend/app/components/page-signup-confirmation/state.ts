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

import {UIRouter, StateParams} from '@uirouter/angularjs';
import {IIgniteNg1StateDeclaration} from '../../types';
import publicTemplate from '../../../views/public.pug';

export type PageSignupConfirmationStateParams = StateParams & {email: string};

state.$inject = ['$uiRouter', '$translate'];

export function state(router: UIRouter, $translate: ng.translate.ITranslateService) {
    router.stateRegistry.register({
        name: 'signup-confirmation',
        url: '/signup-confirmation?{email:string}',
        views: {
            '': {
                template: publicTemplate
            },
            'page@signup-confirmation': {
                component: 'pageSignupConfirmation'
            }
        },
        unsaved: true,
        tfMetaTags: {
            title: $translate.instant('signupConfirmation.documentTitle')
        },
        resolve: {
            email() {
                return router.stateService.transition.params<PageSignupConfirmationStateParams>().email;
            }
        }
    } as IIgniteNg1StateDeclaration);
}
