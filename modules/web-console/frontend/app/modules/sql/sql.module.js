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
import webConsoleNavbar from 'app/components/web-console-navbar';

import NotebookData from './Notebook.data';
import Notebook from './Notebook.service';
import controller from './sql.controller';
import QueriesNavbar from './services/queries-navbar';
import CreateQueryDialog from './services/create-query-dialog';

import sqlTplUrl from 'app/../views/sql/sql.tpl.pug';

angular.module('ignite-console.sql', [
    webConsoleNavbar.name,
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
.service('QueriesNavbar', QueriesNavbar)
.service('CreateQueryDialog', CreateQueryDialog)
.decorator('webConsoleNavbarDirective', ['$delegate', 'QueriesNavbar', function($delegate, QueriesNavbar) {
    $delegate[0].controller.items.push(QueriesNavbar);

    return $delegate;
}])
.service('IgniteNotebookData', NotebookData)
.service('IgniteNotebook', Notebook);
