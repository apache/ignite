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

angular
    .module('ignite-console.states.errors', [
        'ui.router'
    ])
    .config(['$stateProvider', /** @param {import('@uirouter/angularjs').StateProvider} $stateProvider */ function($stateProvider) {
        // set up the states
        $stateProvider
            .state('base.404', {
                url: '/404',
                component: 'timedRedirection',
                resolve: {
                    headerText: () => '404',
                    subHeaderText: () => 'Page not found'
                },
                tfMetaTags: {
                    title: 'Page not found'
                },
                unsaved: true
            })
            .state('base.403', {
                url: '/403',
                component: 'timedRedirection',
                resolve: {
                    headerText: () => '403',
                    subHeaderText: () => 'You are not authorized'
                },
                tfMetaTags: {
                    title: 'Not authorized'
                },
                unsaved: true
            })
            .state('base.503', {
                url: '/503',
                component: 'timedRedirection',
                resolve: {
                    headerText: () => '503',
                    subHeaderText: () => 'Web Console not available'
                },
                tfMetaTags: {
                    title: 'Not available'
                },
                unsaved: true
            });
    }]);
