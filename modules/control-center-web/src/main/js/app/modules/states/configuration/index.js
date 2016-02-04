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
import clustersDeployment from './clusters/deployment.directive';
import clustersConnector from './clusters/connector.directive';
import clustersCommunication from './clusters/communication.directive';
import clustersBinary from './clusters/binary.directive';
import clustersAtomic from './clusters/atomic.directive';

import clustersGeneral from './clusters/general.directive';
import generalDiscoveryCloud from './clusters/general/discovery/cloud.directive';
import generalDiscoveryGoogle from './clusters/general/discovery/google.directive';
import generalDiscoveryJdbc from './clusters/general/discovery/jdbc.directive';
import generalDiscoveryMulticast from './clusters/general/discovery/multicast.directive';
import generalDiscoveryS3 from './clusters/general/discovery/s3.directive';
import generalDiscoveryShared from './clusters/general/discovery/shared.directive';
import generalDiscoveryVm from './clusters/general/discovery/vm.directive';

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
.directive(...clustersDeployment)
.directive(...clustersConnector)
.directive(...clustersCommunication)
.directive(...clustersBinary)
.directive(...clustersAtomic)
.directive(...clustersGeneral)
.directive(...generalDiscoveryCloud)
.directive(...generalDiscoveryGoogle)
.directive(...generalDiscoveryJdbc)
.directive(...generalDiscoveryMulticast)
.directive(...generalDiscoveryS3)
.directive(...generalDiscoveryShared)
.directive(...generalDiscoveryVm)
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
        params: {
            id: null
        },
        data: {
            loading: 'Loading clusters screen...'
        },
        resolve: {
            $title: () => {
                return 'Configure Clusters';
            }
        }
    })
    .state('base.configuration.caches', {
        url: '/caches',
        templateUrl: '/configuration/caches.html',
        params: {
            id: null
        },
        data: {
            loading: 'Loading caches screen...'
        },
        resolve: {
            $title: () => {
                return 'Configure Caches';
            }
        }
    })
    .state('base.configuration.domains', {
        url: '/domains',
        templateUrl: '/configuration/domains.html',
        params: {
            id: null
        },
        data: {
            loading: 'Loading domain models screen...'
        },
        resolve: {
            $title: () => {
                return 'Configure Domain Model';
            }
        }
    })
    .state('base.configuration.igfs', {
        url: '/igfs',
        templateUrl: '/configuration/igfs.html',
        params: {
            id: null
        },
        data: {
            loading: 'Loading IGFS screen...'
        },
        resolve: {
            $title: () => {
                return 'Configure IGFS';
            }
        }
    })
    .state('base.configuration.summary', {
        url: '/summary',
        templateUrl: '/configuration/summary.html',
        controller: ConfigurationSummaryCtrl,
        controllerAs: 'ctrl',
        data: {
            loading: 'Loading summary screen...'
        },
        resolve: {
            $title: () => {
                return 'Configurations Summary';
            }
        }
    });
}]);
