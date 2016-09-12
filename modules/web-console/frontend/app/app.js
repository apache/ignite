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

import '../public/stylesheets/style.scss';
import './helpers/jade/mixins.jade';

import './app.config';

import './decorator/select';
import './decorator/tooltip';

import './modules/form/form.module';
import './modules/agent/agent.module.js';
import './modules/sql/sql.module';
import './modules/Demo/Demo.module.js';

import './modules/states/signin.state';
import './modules/states/logout.state';
import './modules/states/password.state';
import './modules/states/configuration.state';
import './modules/states/profile.state';
import './modules/states/admin.state';
import './modules/states/errors.state';

// ignite:modules
import './modules/user/user.module';
import './modules/branding/branding.module';
import './modules/navbar/navbar.module';
import './modules/configuration/configuration.module';
import './modules/getting-started/GettingStarted.provider';
import './modules/dialog/dialog.module';
import './modules/version/Version.provider';
import './modules/ace.module';
import './modules/socket.module';
import './modules/loading/loading.module';
// endignite

// Directives.
import igniteAutoFocus from './directives/auto-focus.directive.js';
import igniteBsAffixUpdate from './directives/bs-affix-update.directive';
import igniteCentered from './directives/centered/centered.directive.js';
import igniteCopyToClipboard from './directives/copy-to-clipboard.directive.js';
import igniteHideOnStateChange from './directives/hide-on-state-change/hide-on-state-change.directive';
import igniteInformation from './directives/information/information.directive';
import igniteMatch from './directives/match.directive.js';
import igniteOnClickFocus from './directives/on-click-focus.directive.js';
import igniteOnEnter from './directives/on-enter.directive.js';
import igniteOnEnterFocusMove from './directives/on-enter-focus-move.directive.js';
import igniteOnEscape from './directives/on-escape.directive.js';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAceJava from './directives/ui-ace-java/ui-ace-java.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceTabs from './directives/ui-ace-tabs.directive';
import igniteUiAceXml from './directives/ui-ace-xml/ui-ace-xml.directive';

// Services.
import ChartColors from './services/ChartColors.service';
import Clone from './services/Clone.service.js';
import Confirm from './services/Confirm.service.js';
import ConfirmBatch from './services/ConfirmBatch.service.js';
import CopyToClipboard from './services/CopyToClipboard.service';
import Countries from './services/Countries.service';
import Focus from './services/Focus.service';
import InetAddress from './services/InetAddress.service';
import JavaTypes from './services/JavaTypes.service';
import Messages from './services/Messages.service';
import ModelNormalizer from './services/ModelNormalizer.service.js';
import LegacyTable from './services/LegacyTable.service';
import ErrorPopover from './services/ErrorPopover.service';
import FormUtils from './services/FormUtils.service';
import LegacyUtils from './services/LegacyUtils.service';
import UnsavedChangesGuard from './services/UnsavedChangesGuard.service';

// Providers.

// Filters.
import byName from './filters/byName.filter';
import domainsValidation from './filters/domainsValidation.filter';
import hasPojo from './filters/hasPojo.filter';
import duration from './filters/duration.filter';

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

// Controllers
import admin from 'controllers/admin-controller';
import caches from 'controllers/caches-controller';
import clusters from 'controllers/clusters-controller';
import domains from 'controllers/domains-controller';
import igfs from 'controllers/igfs-controller';
import profile from 'controllers/profile-controller';
import auth from './controllers/auth.controller';
import resetPassword from './controllers/reset-password.controller';

// Inject external modules.
import 'ignite_modules_temp/index';

import baseTemplate from '../views/base.jade';

angular
.module('ignite-console', [
    // Optional AngularJS modules.
    'ngAnimate',
    'ngSanitize',
    // Third party libs.
    'ngRetina',
    'btford.socket-io',
    'mgcrea.ngStrap',
    'ui.router',
    'gridster',
    'dndLists',
    'nvd3',
    'smart-table',
    'treeControl',
    'ui.grid',
    'ui.grid.saveState',
    'ui.grid.selection',
    'ui.grid.resizeColumns',
    'ui.grid.autoResize',
    'ui.grid.exporter',
    // Base modules.
    'ignite-console.ace',
    'ignite-console.Form',
    'ignite-console.user',
    'ignite-console.branding',
    'ignite-console.socket',
    'ignite-console.agent',
    'ignite-console.sql',
    'ignite-console.demo',
    // States.
    'ignite-console.states.login',
    'ignite-console.states.logout',
    'ignite-console.states.password',
    'ignite-console.states.configuration',
    'ignite-console.states.profile',
    'ignite-console.states.admin',
    'ignite-console.states.errors',
    // Common modules.
    'ignite-console.dialog',
    'ignite-console.navbar',
    'ignite-console.configuration',
    'ignite-console.getting-started',
    'ignite-console.version',
    'ignite-console.loading',
    // Ignite configuration module.
    'ignite-console.config',
    // Ignite modules.
    'ignite-console.modules'
])
// Directives.
.directive(...igniteAutoFocus)
.directive(...igniteBsAffixUpdate)
.directive(...igniteCentered)
.directive(...igniteCopyToClipboard)
.directive(...igniteHideOnStateChange)
.directive(...igniteInformation)
.directive(...igniteMatch)
.directive(...igniteOnClickFocus)
.directive(...igniteOnEnter)
.directive(...igniteOnEnterFocusMove)
.directive(...igniteOnEscape)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAceJava)
.directive(...igniteUiAcePojos)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceTabs)
.directive(...igniteUiAceXml)
// Services.
.service(...ChartColors)
.service(...Clone)
.service(...Confirm)
.service(...ConfirmBatch)
.service(...CopyToClipboard)
.service(...Countries)
.service(...Focus)
.service(...InetAddress)
.service(...JavaTypes)
.service(...Messages)
.service(...ModelNormalizer)
.service(...LegacyTable)
.service('IgniteErrorPopover', ErrorPopover)
.service(...FormUtils)
.service(...LegacyUtils)
.service(...UnsavedChangesGuard)
// Controllers.
.controller(...admin)
.controller(...auth)
.controller(...resetPassword)
.controller(...caches)
.controller(...clusters)
.controller(...domains)
.controller(...igfs)
.controller(...profile)
// Filters.
.filter(...hasPojo)
.filter(...domainsValidation)
.filter(...byName)
.filter(...duration)
.config(['$stateProvider', '$locationProvider', '$urlRouterProvider', ($stateProvider, $locationProvider, $urlRouterProvider) => {
    // Set up the states.
    $stateProvider
        .state('base', {
            url: '',
            abstract: true,
            templateUrl: baseTemplate
        })
        .state('settings', {
            url: '/settings',
            abstract: true,
            templateUrl: baseTemplate
        });

    $urlRouterProvider.otherwise('/404');
    $locationProvider.html5Mode(true);
}])
.run(['$rootScope', '$state', 'MetaTags', 'gettingStarted', ($root, $state, $meta, gettingStarted) => {
    $root._ = _;
    $root.$state = $state;
    $root.$meta = $meta;
    $root.gettingStarted = gettingStarted;
}])
.run(['$rootScope', 'IgniteAgentMonitor', ($root, agentMonitor) => {
    $root.$on('user', () => agentMonitor.init());
}])
.run(['$rootScope', ($root) => {
    $root.$on('$stateChangeStart', () => {
        _.forEach(angular.element('.modal'), (m) => angular.element(m).scope().$hide());
    });
}])
.run(['$rootScope', '$http', '$state', 'IgniteMessages', 'User',
    ($root, $http, $state, Messages, User) => { // eslint-disable-line no-shadow
        $root.revertIdentity = () => {
            $http.get('/api/v1/admin/revert/identity')
                .then(User.load)
                .then((user) => {
                    $root.$broadcast('user', user);

                    $state.go('settings.admin');
                })
                .catch(Messages.showError);
        };
    }
]);
