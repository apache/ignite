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
import controller from './sql.controller';

import sqlTplUrl from 'app/../views/sql/sql.tpl.pug';

angular.module('ignite-console.sql', [
    'ui.router'
])
.config(['$stateProvider', ($stateProvider) => {
    // set up the states
    $stateProvider
        .state('base.sql', {
            url: '/queries',
            abstract: true,
            template: '<ui-view></ui-view>'
        })
        .state('base.sql.notebook', {
            url: '/notebook/{noteId}',
            templateUrl: sqlTplUrl,
            permission: 'query',
            tfMetaTags: {
                title: 'Query notebook'
            },
            controller,
            controllerAs: '$ctrl'
        })
        .state('base.sql.demo', {
            url: '/demo',
            templateUrl: sqlTplUrl,
            permission: 'query',
            tfMetaTags: {
                title: 'SQL demo'
            },
            controller,
            controllerAs: '$ctrl'
        });
}])
.service('IgniteNotebookData', NotebookData)
.service('IgniteNotebook', Notebook)
.controller('notebookController', notebook);
