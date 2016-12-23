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

// Common directives.
import previewPanel from './configuration/preview-panel.directive.js';

// Summary screen.
import ConfigurationSummaryCtrl from './configuration/summary/summary.controller';
import ConfigurationResource from './configuration/Configuration.resource';
import summaryTabs from './configuration/summary/summary-tabs.directive';
import IgniteSummaryZipper from './configuration/summary/summary-zipper.service';

angular.module('ignite-console.states.configuration', ['ui.router'])
    .directive(...previewPanel)
    // Summary screen
    .directive(...summaryTabs)
    // Services.
    .service('IgniteSummaryZipper', IgniteSummaryZipper)
    .service('IgniteConfigurationResource', ConfigurationResource)
    // Configure state provider.
    .config(['$stateProvider', 'AclRouteProvider', ($stateProvider, AclRoute) => {
        // Setup the states.
        $stateProvider
            .state('base.configuration', {
                url: '/configuration',
                templateUrl: '/configuration/sidebar.html',
                abstract: true
            })
            .state('base.configuration.clusters', {
                url: '/clusters',
                templateUrl: '/configuration/clusters.html',
                onEnter: AclRoute.checkAccess('configuration'),
                params: {
                    linkId: null
                },
                metaTags: {
                    title: 'Configure Clusters'
                }
            })
            .state('base.configuration.caches', {
                url: '/caches',
                templateUrl: '/configuration/caches.html',
                onEnter: AclRoute.checkAccess('configuration'),
                params: {
                    linkId: null
                },
                metaTags: {
                    title: 'Configure Caches'
                }
            })
            .state('base.configuration.domains', {
                url: '/domains',
                templateUrl: '/configuration/domains.html',
                onEnter: AclRoute.checkAccess('configuration'),
                params: {
                    linkId: null
                },
                metaTags: {
                    title: 'Configure Domain Model'
                }
            })
            .state('base.configuration.igfs', {
                url: '/igfs',
                templateUrl: '/configuration/igfs.html',
                onEnter: AclRoute.checkAccess('configuration'),
                params: {
                    linkId: null
                },
                metaTags: {
                    title: 'Configure IGFS'
                }
            })
            .state('base.configuration.summary', {
                url: '/summary',
                templateUrl: '/configuration/summary.html',
                onEnter: AclRoute.checkAccess('configuration'),
                controller: ConfigurationSummaryCtrl,
                controllerAs: 'ctrl',
                metaTags: {
                    title: 'Configurations Summary'
                }
            });
    }]);
