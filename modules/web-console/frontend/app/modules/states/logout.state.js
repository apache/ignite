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
