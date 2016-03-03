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

import ConfigurationSummaryCtrl from './configuration/summary/summary.controller';
import ConfigurationSummaryResource from './configuration/summary/summary.resource';
import summaryTabs from './configuration/summary/summary-tabs.directive';
import previewPanel from './configuration/clusters/preview-panel.directive';

import clustersTransactions from './configuration/clusters/transactions.directive';
import clustersThread from './configuration/clusters/thread.directive';
import clustersTime from './configuration/clusters/time.directive';
import clustersSwap from './configuration/clusters/swap.directive';
import clustersSsl from './configuration/clusters/ssl.directive';
import clustersMetrics from './configuration/clusters/metrics.directive';
import clustersMarshaller from './configuration/clusters/marshaller.directive';
import clustersIgfs from './configuration/clusters/igfs.directive';
import clustersEvents from './configuration/clusters/events.directive';
import clustersDiscovery from './configuration/clusters/discovery.directive';
import clustersDeployment from './configuration/clusters/deployment.directive';
import clustersConnector from './configuration/clusters/connector.directive';
import clustersCommunication from './configuration/clusters/communication.directive';
import clustersBinary from './configuration/clusters/binary.directive';
import clustersAtomic from './configuration/clusters/atomic.directive';

import clustersGeneral from './configuration/clusters/general.directive';
import generalDiscoveryCloud from './configuration/clusters/general/discovery/cloud.directive';
import generalDiscoveryGoogle from './configuration/clusters/general/discovery/google.directive';
import generalDiscoveryJdbc from './configuration/clusters/general/discovery/jdbc.directive';
import generalDiscoveryMulticast from './configuration/clusters/general/discovery/multicast.directive';
import generalDiscoveryS3 from './configuration/clusters/general/discovery/s3.directive';
import generalDiscoveryShared from './configuration/clusters/general/discovery/shared.directive';
import generalDiscoveryVm from './configuration/clusters/general/discovery/vm.directive';
import generalDiscoveryZookeeper from './configuration/clusters/general/discovery/zookeeper.directive';

import generalDiscoveryZookeeperRetryExponential from './configuration/clusters/general/discovery/zookeeper/retrypolicy/exponential-backoff.directive';
import generalDiscoveryZookeeperRetryBoundedExponential from './configuration/clusters/general/discovery/zookeeper/retrypolicy/bounded-exponential-backoff.directive';
import generalDiscoveryZookeeperRetryUntilElapsed from './configuration/clusters/general/discovery/zookeeper/retrypolicy/until-elapsed.directive';
import generalDiscoveryZookeeperRetryNTimes from './configuration/clusters/general/discovery/zookeeper/retrypolicy/n-times.directive';
import generalDiscoveryZookeeperRetryOneTime from './configuration/clusters/general/discovery/zookeeper/retrypolicy/one-time.directive';
import generalDiscoveryZookeeperRetryForever from './configuration/clusters/general/discovery/zookeeper/retrypolicy/forever.directive';
import generalDiscoveryZookeeperRetryCustom from './configuration/clusters/general/discovery/zookeeper/retrypolicy/custom.directive';

angular
.module('ignite-console.states.configuration', [
    'ui.router'
])
.directive(...summaryTabs)
.directive(...previewPanel)
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
.directive(...generalDiscoveryZookeeper)
.directive(...generalDiscoveryZookeeperRetryExponential)
.directive(...generalDiscoveryZookeeperRetryBoundedExponential)
.directive(...generalDiscoveryZookeeperRetryUntilElapsed)
.directive(...generalDiscoveryZookeeperRetryNTimes)
.directive(...generalDiscoveryZookeeperRetryOneTime)
.directive(...generalDiscoveryZookeeperRetryForever)
.directive(...generalDiscoveryZookeeperRetryCustom)
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
        metaTags: {
            title: 'Configure Clusters'
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
        metaTags: {
            title: 'Configure Caches'
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
        metaTags: {
            title: 'Configure Domain Model'
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
        metaTags: {
            title: 'Configure IGFS'
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
        metaTags: {
            title: 'Configurations Summary'
        }
    });
}]);
