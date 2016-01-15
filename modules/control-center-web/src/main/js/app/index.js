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

import jQuery from 'jquery';
import _ from 'lodash';
import ace from 'ace';
import angular from 'angular';
import pdfMake from 'pdfmake';

import 'angular-ui-router';
import 'angular-ui-router-title';
import 'angular-strap';
import 'angular-ui-ace';
import 'angular-tree-control';
import 'angular-smart-table';
import 'angular-animate';
import 'angular-sanitize';
import 'angular-ui-grid';
import 'angular-loading';
import 'angular-drag-and-drop-lists';
import 'angular-nvd3';

import 'bootstrap-carousel';
import 'file-saver';
import 'jszip';
import 'query-command-supported';

import 'public/stylesheets/style.css!';

import 'nvd3/build/nv.d3.css!';
import 'angular-tree-control/css/tree-control-attribute.css!';
import 'angular-tree-control/css/tree-control.css!';
import 'angular-ui-grid/ui-grid.css!';
import 'angular-loading/angular-loading.css!';
import 'angular-motion/dist/angular-motion.css!';

window._ = _;
window.jQuery = jQuery;
window.ace = ace;
window.require = ace.require;
window.angular = angular;
window.pdfMake = pdfMake;

import './modules/User/index';
import './modules/Auth/index';
import './modules/Form/index';
import './modules/JavaTypes/index';
import './modules/QueryNotebooks/index';

import './modules/states/login/index';
import './modules/states/logout/index';
import './modules/states/password/index';
import './modules/states/configuration/index';
import './modules/states/sql/index';
import './modules/states/profile/index';
import './modules/states/admin/index';

// ignite:modules
import './modules/dialog/index';
import './modules/navbar/main';
import './modules/settings/main';
import './modules/configuration/sidebar/main';
import './modules/configuration/include-event-types/main';
import './modules/terms/main';
// endignite

// Directives.
import igniteLoading from './directives/loading/loading.directive';
import igniteCallout from './directives/callout/callout.directive';
import igniteCalloutCel from './directives/callout/callout-cel.directive';
import igniteUiAceXml from './directives/ui-ace-xml/ui-ace-xml.directive';
import igniteUiAceJava from './directives/ui-ace-java/ui-ace-java.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';

// Services.
import IgniteUiAceOnLoad from './services/UiAceOnLoad/service';

// Filters.
import hasPojo from './filters/hasPojo/hasPojo.filter';

angular
.module('ignite-console', [
    'ui.router',
    'ui.router.title',
    // Base modules.
    'ignite-console.Auth',
    'ignite-console.User',
    'ignite-console.Form',
    'ignite-console.JavaTypes',
    'ignite-console.QueryNotebooks',
    // States.
    'ignite-console.states.login',
    'ignite-console.states.logout',
    'ignite-console.states.password',
    'ignite-console.states.configuration',
    'ignite-console.states.sql',
    'ignite-console.states.profile',
    'ignite-console.states.admin',
    // Common modules.
    'ignite-console.dialog',
    'ignite-console.navbar',
    'ignite-console.userbar',
    'ignite-console.configuration.sidebar',
    'ignite-console.configuration.include-event-types',
    'ignite-console.terms'
])
// Directives.
.directive(...igniteLoading)
.directive(...igniteCallout)
.directive(...igniteCalloutCel)
.directive(...igniteUiAceXml)
.directive(...igniteUiAceJava)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAcePojos)
// Services.
.service(...IgniteUiAceOnLoad)
// Filters.
.filter(...hasPojo)
.config(['$stateProvider', '$locationProvider', '$urlRouterProvider', function($stateProvider, $locationProvider, $urlRouterProvider) {
    // Set up the states.
    $stateProvider
        .state('base', {
            url: '',
            abstract: true,
            templateUrl: '/base.html'
        })
        .state('settings', {
            url: '/settings',
            abstract: true,
            templateUrl: '/base.html'
        });

    $urlRouterProvider.when('/', '/configuration/clusters');

    $locationProvider.html5Mode(true);
}])
.run(['$rootScope', '$state', ($root, $state) => {
    $root.$state = $state;
}]);
