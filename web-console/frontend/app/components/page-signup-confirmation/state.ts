

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
