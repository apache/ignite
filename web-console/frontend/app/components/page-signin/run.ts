

import publicTemplate from '../../../views/public.pug';
import {UIRouter, StateParams} from '@uirouter/angularjs';
import {IIgniteNg1StateDeclaration} from 'app/types';

export type PageSigninStateParams = StateParams & {activationToken?: string};

registerState.$inject = ['$uiRouter', '$translate'];

export function registerState($uiRouter: UIRouter, $translate: ng.translate.ITranslateService) {
    const state: IIgniteNg1StateDeclaration = {
        url: '/signin?{activationToken:string}',
        name: 'signin',
        views: {
            '': {
                template: publicTemplate
            },
            'page@signin': {
                component: 'pageSignin'
            }
        },
        unsaved: true,
        redirectTo: (trans) => {
            const skipStates = new Set(['signup', 'forgotPassword', 'landing']);

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
            title: $translate.instant('signIn.documentTitle')
        },
        resolve: {
            activationToken() {
                return $uiRouter.stateService.transition.params<PageSigninStateParams>().activationToken;
            }
        }
    };

    $uiRouter.stateRegistry.register(state);
}
