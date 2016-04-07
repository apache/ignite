/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import angular from 'angular';

angular
.module('ignite-console.demo', [
    'ignite-console.socket'
])
.provider('Demo', ['igniteSocketFactoryProvider', '$httpProvider', function(igniteSocketFactoryProvider, $httpProvider) {
    const enabled = sessionStorage.getItem('IgniteDemoMode') === 'true';

    if (enabled) {
        sessionStorage.setItem('IgniteDemoMode', 'true');

        igniteSocketFactoryProvider.set({query: 'IgniteDemoMode=true'});

        $httpProvider.interceptors.push('demoInterceptor');
    }

    this.$get = ['$rootScope', ($root) => {
        $root.IgniteDemoMode = enabled;

        return {enabled};
    }];
}])
.factory('demoInterceptor', ['Demo', (Demo) => {
    const isApiRequest = (url) => /\/api\/v1/ig.test(url);

    return {
        request(cfg) {
            if (Demo.enabled && isApiRequest(cfg.url))
                cfg.headers.IgniteDemoMode = true;

            return cfg;
        }
    };
}])
.controller('demoController', ['$scope', '$state', '$window', '$http', '$confirm', '$common', ($scope, $state, $window, $http, $confirm, $common) => {
    $scope.toggleDemo = () => {
        const enabled = sessionStorage.getItem('IgniteDemoMode') === 'true';

        sessionStorage.setItem('IgniteDemoMode', !enabled);

        const url = $state.href($state.current.name, $state.params);

        $window.open(url, '_self');
    };

    const _resetDemo = () => {
        $http.post('/api/v1/demo/reset')
            .then($scope.toggleDemo)
            .catch((errMsg) => $common.showError(errMsg));
    };

    $scope.startDemo = () => {
        if (!$scope.user.demoCreated)
            return _resetDemo();

        $confirm.confirm('Would you like to continue with previous demo session?', true)
            .then((resume) => {
                if (resume)
                    return $scope.toggleDemo();

                _resetDemo();
            });
    };
}]);
