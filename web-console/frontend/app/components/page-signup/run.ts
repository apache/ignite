

import publicTemplate from '../../../views/public.pug';
import {IIgniteNg1StateDeclaration} from 'app/types';
import {UIRouter} from '@uirouter/angularjs';

registerState.$inject = ['$uiRouter', '$translate'];

export function registerState($uiRouter: UIRouter, $translate: ng.translate.ITranslateService) {
    const state: IIgniteNg1StateDeclaration = {
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
            title: $translate.instant('signUp.documentTitle')
        }
    };
    $uiRouter.stateRegistry.register(state);
}
