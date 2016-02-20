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

window._ = _;
window.jQuery = jQuery;
window.ace = ace;
window.require = ace.require; // TODO Should be removed after full refactoring to directives.
window.angular = angular;
window.pdfMake = pdfMake;

import 'angular-ui-router';
import 'angular-ui-router-title';
import 'angular-animate';
import 'angular-sanitize';
import 'angular-ui-grid';
import 'angular-loading';
import 'angular-drag-and-drop-lists';
import 'angular-nvd3';
import 'angular-retina';
import 'angular-strap';
import 'angular-ui-ace';
import 'angular-tree-control';
import 'angular-smart-table';

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

// import './decorator/select';

import './modules/form/form.module';
import './modules/JavaTypes/JavaTypes.provider';
import './modules/QueryNotebooks/QueryNotebooks.provider';

import './modules/states/signin.state';
import './modules/states/logout.state';
import './modules/states/password.state';
import './modules/states/configuration.state';
import './modules/states/sql.state';
import './modules/states/profile.state';
import './modules/states/admin.state';

// ignite:modules
import './modules/user/user.module';
import './modules/branding/branding.module';
import './modules/navbar/navbar.module';
import './modules/configuration/configuration.module';
import './modules/getting-started/GettingStarted.provider';
import './modules/dialog/dialog.module';
import './modules/Version/Version.provider';
// endignite

// Directives.
import igniteLoading from './directives/loading/loading.directive';
import igniteInformation from './directives/information/information.directive';
import igniteUiAce from './directives/ui-ace/ui-ace.directive';
import igniteUiAceXml from './directives/ui-ace-xml/ui-ace-xml.directive';
import igniteUiAceJava from './directives/ui-ace-java/ui-ace-java.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteFormFieldJavaClass from './directives/form-field-java-class/form-field-java-class.directive';
import igniteBsAffixUpdate from './directives/bs-affix-update/bs-affix-update.directive';

// Services.
import cleanup from './services/cleanup/cleanup.service';
import GeneratorXml from './services/Generator/Xml.service';
import GeneratorJava from './services/Generator/Java.service';
import IgniteCountries from './services/Countries/Countries.service';

// Providers

// Filters.
import hasPojo from './filters/hasPojo/hasPojo.filter';

angular
.module('ignite-console', [
    'ui.router',
    'ui.router.title',
    'ngRetina',
    // Base modules.
    'ignite-console.user',
    'ignite-console.branding',
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
    'ignite-console.configuration',
    'ignite-console.getting-started',
    'ignite-console.version'
])
// Directives.
.directive(...igniteLoading)
.directive(...igniteInformation)
.directive(...igniteUiAce)
.directive(...igniteUiAceXml)
.directive(...igniteUiAceJava)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAcePojos)
.directive(...igniteFormFieldJavaClass)
.directive(...igniteBsAffixUpdate)
// Services.
.service(...cleanup)
.service(...GeneratorXml)
.service(...GeneratorJava)
.service(...IgniteCountries)
// Providers.
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

    $urlRouterProvider.when('/', '/signin');

    $locationProvider.html5Mode(true);
}])
.run(['$rootScope', ($root) => {
    $root._ = _;
}])
.run(['$rootScope', '$state', 'Auth', 'User', ($root, $state, Auth, User) => {
    $root.$state = $state;

    if (Auth.authorized) {
        User.read()
            .then((user) => $root.$broadcast('user', user));
    }
}])
.run(['$rootScope', ($root) => {
    $root.$on('$stateChangeStart', () => {
        _.each(angular.element('.modal'), (m) => angular.element(m).scope().$hide());
    });
}]);
