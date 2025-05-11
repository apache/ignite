

import angular from 'angular';

angular.module('ignite-console.states.logout', [
    'ui.router'
])
.config(['$stateProvider', /** @param {import('@uirouter/angularjs').StateProvider} $stateProvider */ function($stateProvider) {
    // set up the states
    $stateProvider.state('logout', {
        url: '/logout',
        permission: 'logout',
        controller: ['Auth', function(Auth) {Auth.logout();}],
        tfMetaTags: {
            title: 'Logout'
        },
        template: `<div class='splash'><div class='splash-wrapper'><div class='spinner'><div class='bounce1'></div><div class='bounce2'></div><div class='bounce3'></div></div><div class='splash-wellcome'>Logout...</div></div></div>`
    });
}]);
