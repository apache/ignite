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

import ConfigurationSummaryCtrl from './summary/summary.controller';
import ConfigurationSummaryResource from './summary/summary.resource';

import clustersTransactions from './clusters/transactions.directive';
import clustersThread from './clusters/thread.directive';
import clustersTime from './clusters/time.directive';
import clustersSwap from './clusters/swap.directive';
import clustersSsl from './clusters/ssl.directive';
import clustersMetrics from './clusters/metrics.directive';
import clustersMarshaller from './clusters/marshaller.directive';
import clustersIgfs from './clusters/igfs.directive';
import clustersEvents from './clusters/events.directive';
import clustersDiscovery from './clusters/discovery.directive';

angular
.module('ignite-console.states.configuration', [
    'ui.router'
])
.directive(...clustersTransactions)
.directive(...clustersThread)
.directive(...clustersTime)
.directive(...clustersSwap)
.directive(...clustersSsl)
.directive(...clustersMetrics)
.directive(...clustersMarshaller)
.directive(...clustersIgfs)
.directive(...clustersEvents)
.directive(...clustersDiscovery)
// Services.
.service(...ConfigurationSummaryResource)
.config(['$stateProvider', function($stateProvider) {
    // Setup the states.
    $stateProvider
    .state('base.configuration', {
        url: '/configuration',
        templateUrl: '/configuration/sidebar.html'
    })
    .state('base.configuration.clusters', {
        url: '/clusters',
        templateUrl: '/configuration/clusters.html',
        data: {
            loading: 'Loading clusters screen...'
        }
    })
    .state('base.configuration.caches', {
        url: '/caches',
        templateUrl: '/configuration/caches.html',
        data: {
            loading: 'Loading caches screen...'
        }
    })
    .state('base.configuration.metadata', {
        url: '/metadata',
        templateUrl: '/configuration/metadata.html',
        data: {
            loading: 'Loading metadata screen...'
        }
    })
    .state('base.configuration.igfs', {
        url: '/igfs',
        templateUrl: '/configuration/igfs.html',
        data: {
            loading: 'Loading IGFS screen...'
        }
    })
    .state('base.configuration.summary', {
        url: '/summary',
        templateUrl: '/configuration/summary.html',
        controller: ConfigurationSummaryCtrl,
        controllerAs: 'ctrl',
        data: {
            loading: 'Loading summary screen...'
        }
    });
}]);
