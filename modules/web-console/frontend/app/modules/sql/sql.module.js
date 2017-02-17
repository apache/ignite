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

import NotebookData from './Notebook.data';
import Notebook from './Notebook.service';
import notebook from './notebook.controller';
import sql from './sql.controller';

angular.module('ignite-console.sql', [
    'ui.router'
])
    .config(['$stateProvider', 'AclRouteProvider',
        ($stateProvider, AclRoute) => {
            // set up the states
            $stateProvider
                .state('base.sql', {
                    url: '/queries',
                    abstract: true,
                    template: '<ui-view></ui-view>'
                })
                .state('base.sql.notebook', {
                    url: '/notebook/{noteId}',
                    templateUrl: '/sql/sql.html',
                    onEnter: AclRoute.checkAccess('query'),
                    metaTags: {
                        title: 'Query notebook'
                    }
                })
                .state('base.sql.demo', {
                    url: '/demo',
                    templateUrl: '/sql/sql.html',
                    onEnter: AclRoute.checkAccess('query'),
                    metaTags: {
                        title: 'SQL demo'
                    }
                });
        }]
    )
    .service('IgniteNotebookData', NotebookData)
    .service('IgniteNotebook', Notebook)
    .controller('notebookController', notebook)
    .controller('sqlController', sql);
