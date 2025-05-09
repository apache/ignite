

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
