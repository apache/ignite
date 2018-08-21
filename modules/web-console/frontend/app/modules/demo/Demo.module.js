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

import DEMO_INFO from 'app/data/demo-info.json';
import templateUrl from 'views/templates/demo-info.tpl.pug';

angular
.module('ignite-console.demo', [
    'ignite-console.socket'
])
.config(['$stateProvider', ($stateProvider) => {
    $stateProvider
        .state('demo', {
            abstract: true,
            url: '/demo',
            template: '<ui-view></ui-view>'
        })
        .state('demo.resume', {
            url: '/resume',
            permission: 'demo',
            redirectTo: 'default-state',
            unsaved: true,
            tfMetaTags: {
                title: 'Demo resume'
            }
        })
        .state('demo.reset', {
            url: '/reset',
            permission: 'demo',
            redirectTo: (trans) => {
                const $http = trans.injector().get('$http');

                return $http.post('/api/v1/demo/reset')
                    .then(() => 'default-state')
                    .catch((err) => {
                        trans.injector().get('IgniteMessages').showError(err);

                        return 'default-state';
                    });
            },
            unsaved: true,
            tfMetaTags: {
                title: 'Demo reset'
            }
        });
}])
.provider('Demo', ['$stateProvider', '$httpProvider', 'igniteSocketFactoryProvider', function($state, $http, socketFactory) {
    if (/(\/demo.*)/ig.test(location.pathname))
        sessionStorage.setItem('IgniteDemoMode', 'true');

    const enabled = sessionStorage.getItem('IgniteDemoMode') === 'true';

    if (enabled) {
        socketFactory.set({query: 'IgniteDemoMode=true'});

        $http.interceptors.push('demoInterceptor');
    }

    this.$get = ['$rootScope', ($root) => {
        $root.IgniteDemoMode = enabled;

        return {enabled};
    }];
}])
.factory('demoInterceptor', ['Demo', function(Demo) {
    const isApiRequest = (url) => /\/api\/v1/ig.test(url);

    return {
        request(cfg) {
            if (Demo.enabled && isApiRequest(cfg.url))
                cfg.headers.IgniteDemoMode = true;

            return cfg;
        }
    };
}])
.controller('demoController', ['$scope', '$state', '$window', 'IgniteConfirm', function($scope, $state, $window, Confirm) {
    const _openTab = (stateName) => $window.open($state.href(stateName), '_blank');

    $scope.startDemo = () => {
        if (!$scope.user.demoCreated)
            return _openTab('demo.reset');

        Confirm.confirm('Would you like to continue with previous demo session?', true, false)
            .then((resume) => {
                if (resume)
                    return _openTab('demo.resume');

                _openTab('demo.reset');
            });
    };

    $scope.closeDemo = () => {
        $window.close();
    };
}])
.provider('igniteDemoInfo', [function() {
    const items = DEMO_INFO;

    this.update = (data) => items[0] = data;

    this.$get = [() => {
        return items;
    }];
}])
.service('DemoInfo', ['$rootScope', '$modal', '$state', '$q', 'igniteDemoInfo', 'AgentManager', function($rootScope, $modal, $state, $q, igniteDemoInfo, agentMgr) {
    const scope = $rootScope.$new();

    let closePromise = null;

    function _fillPage() {
        const model = igniteDemoInfo;

        scope.title = model[0].title;
        scope.message = model[0].message.join(' ');
    }

    const dialog = $modal({
        templateUrl,
        scope,
        show: false,
        backdrop: 'static'
    });

    scope.close = () => {
        dialog.hide();

        closePromise && closePromise.resolve();
    };

    scope.downloadAgent = () => {
        const lnk = document.createElement('a');

        lnk.setAttribute('href', '/api/v1/agent/downloads/agent');
        lnk.setAttribute('target', '_self');
        lnk.setAttribute('download', null);
        lnk.style.display = 'none';

        document.body.appendChild(lnk);

        lnk.click();

        document.body.removeChild(lnk);
    };

    return {
        show: () => {
            closePromise = $q.defer();

            _fillPage();

            return dialog.$promise
                .then(dialog.show)
                .then(() => Promise.race([agentMgr.awaitCluster(), closePromise.promise]))
                .then(() => scope.hasAgents = true);
        }
    };
}]);
