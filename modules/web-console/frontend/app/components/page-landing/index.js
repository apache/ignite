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

import angular from 'angular';

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
    .config(['$stateProvider', function($stateProvider) {
        // set up the states
        $stateProvider
        .state('landing', {
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
