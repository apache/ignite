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

import queriesNotebook from './components/queries-notebook';
import pageQueriesCmp from './component';

export default angular.module('ignite-console.sql', [
    'ui.router',
    queriesNotebook.name
])
    .component('pageQueries', pageQueriesCmp)
    .config(['$stateProvider', ($stateProvider) => {
        // set up the states
        $stateProvider
            .state('base.sql', {
                abstract: true,
                template: '<ui-view></ui-view>'
            })
            .state('base.sql.tabs', {
                url: '/queries',
                template: '<page-queries></page-queries>',
                redirectTo: 'base.sql.tabs.notebook',
                permission: 'query'
            })
            .state('base.sql.tabs.notebook', {
                url: '/notebook/{noteId}',
                component: 'queriesNotebook',
                permission: 'query',
                tfMetaTags: {
                    title: 'Query notebook'
                }
            })
            .state('base.sql.tabs.demo', {
                url: '/demo',
                component: 'queriesNotebook',
                permission: 'query',
                tfMetaTags: {
                    title: 'SQL demo'
                }
            });
    }]);
