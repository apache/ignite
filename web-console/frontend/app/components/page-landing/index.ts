

import angular from 'angular';
import {StateRegistry} from '@uirouter/angularjs';

import baseTemplate from './public.pug';
import template from './template.pug';
import './style.scss';

export default angular
    .module('ignite-console.landing', [
        'ui.router',
        'ignite-console.user'
    ])
    .component('pageLanding', {
        template
    })
    .run(['$stateRegistry', function($stateRegistry: StateRegistry) {
        // set up the states
        $stateRegistry.register({
            name: 'landing',
            url: '/',
            views: {
                '@': {
                    template: baseTemplate
                },
                'page@landing': {
                    component: 'pageLanding'
                }
            },
            // template: '<page-landing></page-landing>',
            redirectTo: (trans) => {
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
            unsaved: true
        });
    }]);
