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
import {StateProvider} from '@uirouter/angularjs';

import DEMO_INFO from 'app/data/demo-info.json';
import templateUrl from 'views/templates/demo-info.tpl.pug';
import {directive as demoStatus} from './demoStatus.directive';

const DEMO_QUERY_STATE = {state: 'base.sql.notebook', params: {noteId: 'demo'}};

DemoProvider.$inject = ['$stateProvider', '$httpProvider'];

export function DemoProvider($state: StateProvider, $http: ng.IHttpProvider) {
    if (/(\/demo.*)/ig.test(location.pathname))
        sessionStorage.setItem('demoMode', 'true');

    const enabled = sessionStorage.getItem('demoMode') === 'true';

    if (enabled)
        $http.interceptors.push('demoInterceptor');

    function service(): DemoService {
        return {enabled};
    }

    this.$get = service;
    return this;
}

export interface DemoService {
    enabled: boolean
}


/**
 * @param {{enabled: boolean}} Demo
 * @returns {ng.IHttpInterceptor}
 */
function demoInterceptor(Demo) {
    const isApiRequest = (url) => /\/api\/v1/ig.test(url);

    return {
        request(cfg) {
            if (Demo.enabled && isApiRequest(cfg.url))
                cfg.headers.demoMode = true;

            return cfg;
        }
    };
}

demoInterceptor.$inject = ['Demo'];



function igniteDemoInfoProvider() {
    const items = DEMO_INFO;

    this.update = (data) => items[0] = data;

    this.$get = () => {
        return items;
    };
    return this;
}

/**
 * @param {ng.IRootScopeService} $rootScope
 * @param {mgcrea.ngStrap.modal.IModalScope} $modal
 * @param {import('@uirouter/angularjs').StateService} $state
 * @param {ng.IQService} $q
 * @param {Array<{title: string, message: Array<string>}>} igniteDemoInfo
 * @param {import('app/modules/agent/AgentManager.service').default} agentMgr
 */
function DemoInfo($rootScope, $modal, $state, $q, igniteDemoInfo, agentMgr) {
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

    scope.downloadAgentHref = '/api/v1/downloads/agent';

    scope.close = () => {
        dialog.hide();

        closePromise && closePromise.resolve();
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
}

DemoInfo.$inject = ['$rootScope', '$modal', '$state', '$q', 'igniteDemoInfo', 'AgentManager'];

/**
 * @param {import('@uirouter/angularjs').StateProvider} $stateProvider
 */
function config($stateProvider) {
    $stateProvider
        .state('demo', {
            abstract: true,
            url: '/demo',
            template: '<ui-view></ui-view>'
        })
        .state('demo.resume', {
            url: '/resume',
            permission: 'demo',
            redirectTo: DEMO_QUERY_STATE,
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
                    .then(() => DEMO_QUERY_STATE)
                    .catch((err) => {
                        trans.injector().get('IgniteMessages').showError(err);

                        return DEMO_QUERY_STATE;
                    });
            },
            unsaved: true,
            tfMetaTags: {
                title: 'Demo reset'
            }
        });
}

config.$inject = ['$stateProvider'];

angular
    .module('ignite-console.demo', [])
    .config(config)
    .provider('Demo', DemoProvider)
    .factory('demoInterceptor', demoInterceptor)
    .provider('igniteDemoInfo', igniteDemoInfoProvider)
    .service('DemoInfo', DemoInfo)
    .directive('demoStatus', demoStatus);
