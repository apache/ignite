

import angular from 'angular';

angular
.module('ignite-console.states.admin', [
    'ui.router'
])
.config(['$stateProvider', /** @param {import('@uirouter/angularjs').StateProvider} $stateProvider */ function($stateProvider) {
    // set up the states
    $stateProvider
    .state('base.settings.admin', {
        url: '/admin',
        component: 'pageAdmin',
        permission: 'admin_page',
        tfMetaTags: {
            title: 'Admin panel'
        }
    });
}]);
