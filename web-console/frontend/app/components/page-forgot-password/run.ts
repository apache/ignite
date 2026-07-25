

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
