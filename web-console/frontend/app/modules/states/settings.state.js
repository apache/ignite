


import angular from 'angular';

angular
    .module('ignite-console.states.settings', [
        'ui.router'
    ])
    .config(['$stateProvider', /** @param {import('@uirouter/angularjs').StateProvider} $stateProvider */ function($stateProvider) {
        // Set up the states.
        $stateProvider
            .state('base.settings', {
                url: '/settings',
                abstract: true,
                template: '<ui-view></ui-view>'
            });
    }]);
