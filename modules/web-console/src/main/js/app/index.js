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

import _ from 'lodash';
import ace from 'ace';
import angular from 'angular';
import pdfMake from 'pdfmake';

ace.config.set('basePath', '/jspm_packages/github/ajaxorg/ace-builds@1.2.3');

window._ = _;
window.require = ace.require; // TODO Should be removed after full refactoring to directives.
window.pdfMake = pdfMake;

import 'angular-animate';
import 'angular-sanitize';
import 'angular-strap';
import 'angular-socket-io';
import 'angular-retina';
import 'angular-ui-router';
import 'angular-ui-router-metatags';
import 'angular-smart-table';
import 'angular-ui-grid';
import 'angular-drag-and-drop-lists';
import 'angular-nvd3';
import 'angular-tree-control';
import 'angular-gridster';

import 'bootstrap-carousel';
import 'file-saver';
import 'jszip';
import 'query-command-supported';

import 'public/stylesheets/style.css!';

import 'angular-gridster/dist/angular-gridster.min.css!';
import 'angular-tree-control/css/tree-control-attribute.css!';
import 'angular-tree-control/css/tree-control.css!';
import 'angular-ui-grid/ui-grid.css!';
import 'angular-motion/dist/angular-motion.css!';

import './decorator/select';
import './decorator/tooltip';

import './modules/Demo/Demo.module';
import './modules/form/form.module';
import './services/JavaTypes.service.js';
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
import './modules/ace.module';
import './modules/socket.module';
import './modules/loading/loading.module';
// endignite

// Directives.
import igniteHideOnStateChange from './directives/hide-on-state-change/hide-on-state-change.directive';
import igniteInformation from './directives/information/information.directive';
import igniteUiAceTabs from './directives/ui-ace-tabs.directive';
import igniteUiAceXml from './directives/ui-ace-xml/ui-ace-xml.directive';
import igniteUiAceJava from './directives/ui-ace-java/ui-ace-java.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteBsAffixUpdate from './directives/bs-affix-update.directive';
import igniteСentered from './directives/centered/centered.directive.js';

// Services.
import cleanup from './services/cleanup.service';
import confirm from './services/confirm.service';
import IgniteInetAddress from './services/InetAddress.service';
import IgniteCountries from './services/Countries.service';
import IgniteChartColors from './services/ChartColors.service';
import IgniteAgentMonitor from './services/AgentMonitor.service';
import JavaTypes from './services/JavaTypes.service';

// Providers.

// Filters.
import hasPojo from './filters/hasPojo.filter';
import byName from './filters/byName.filter';

// Generators
import $generatorCommon from 'generator/generator-common';
import $generatorJava from 'generator/generator-java';
import $generatorOptional from 'generator/generator-optional';
import $generatorProperties from 'generator/generator-properties';
import $generatorReadme from 'generator/generator-readme';
import $generatorXml from 'generator/generator-xml';

window.$generatorCommon = $generatorCommon;
window.$generatorJava = $generatorJava;
window.$generatorOptional = $generatorOptional;
window.$generatorProperties = $generatorProperties;
window.$generatorReadme = $generatorReadme;
window.$generatorXml = $generatorXml;

// Add legacy logic;
import consoleModule from 'controllers/common-module';
window.consoleModule = consoleModule;

import 'controllers/admin-controller';
import 'controllers/caches-controller';
import 'controllers/clusters-controller';
import 'controllers/domains-controller';
import 'controllers/igfs-controller';
import 'controllers/profile-controller';
import 'controllers/sql-controller';

// Inject external modules.
import 'ignite_modules_temp/index';

angular
.module('ignite-console', [
    'ngRetina',
    'btford.socket-io',
    'ngAnimate',
    'ngSanitize',
    'mgcrea.ngStrap',
    'ui.router',
    'gridster',
    // Base modules.
    'ignite-console.user',
    'ignite-console.branding',
    'ignite-console.Form',
    'ignite-console.QueryNotebooks',
    'ignite-console.ace',
    'ignite-console.demo',
    'ignite-console.socket',
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
    'ignite-console.version',
    'ignite-console.loading',
    // Ignite legacy module.
    'ignite-console.legacy',
    // Ignite modules.
    'ignite-console.modules'
])
// Directives.
.directive(...igniteHideOnStateChange)
.directive(...igniteInformation)
.directive(...igniteUiAceTabs)
.directive(...igniteUiAceXml)
.directive(...igniteUiAceJava)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAcePojos)
.directive(...igniteBsAffixUpdate)
.directive(...igniteСentered)
// Services.
.service(...cleanup)
.service(...confirm)
.service(...IgniteInetAddress)
.service(...IgniteCountries)
.service(...IgniteChartColors)
.service(...IgniteAgentMonitor)
.service(...JavaTypes)
// Providers.
// Filters.
.filter(...hasPojo)
.filter(...byName)
.config(['$stateProvider', '$locationProvider', '$urlRouterProvider', ($stateProvider, $locationProvider, $urlRouterProvider) => {
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

    $urlRouterProvider.otherwise('/');

    $locationProvider.html5Mode(true);
}])
.config(['$animateProvider', ($animateProvider) => {
    $animateProvider.classNameFilter(/^((?!(fa-spin)).)*$/);
}])
.run(['$rootScope', ($root) => {
    $root._ = _;
}])
.run(['$rootScope', '$state', 'MetaTags', 'Auth', 'User', 'IgniteAgentMonitor', ($root, $state, $meta, Auth, User, agentMonitor) => {
    $root.$state = $state;

    $root.$meta = $meta;

    if (Auth.authorized) {
        User.read()
            .then((user) => $root.$broadcast('user', user))
            .then(() => Auth.authorized && agentMonitor.init());
    }
}])
.run(['$rootScope', ($root) => {
    $root.$on('$stateChangeStart', () => {
        _.forEach(angular.element('.modal'), (m) => angular.element(m).scope().$hide());
    });
}]);
