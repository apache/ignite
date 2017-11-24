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
import '../app/primitives';

import './app.config';

import './modules/form/form.module';
import './modules/agent/agent.module';
import './modules/sql/sql.module';
import './modules/nodes/nodes.module';
import './modules/demo/Demo.module';

import './modules/states/signin.state';
import './modules/states/logout.state';
import './modules/states/password.state';
import './modules/states/configuration.state';
import './modules/states/profile.state';
import './modules/states/admin.state';
import './modules/states/errors.state';

// ignite:modules
import './core';
import './modules/user/user.module';
import './modules/branding/branding.module';
import './modules/navbar/navbar.module';
import './modules/configuration/configuration.module';
import './modules/getting-started/GettingStarted.provider';
import './modules/dialog/dialog.module';
import './modules/ace.module';
import './modules/socket.module';
import './modules/loading/loading.module';
// endignite

// Data
import i18n from './data/i18n';

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
import igniteOnFocusOut from './directives/on-focus-out.directive.js';
import igniteRestoreInputFocus from './directives/restore-input-focus.directive.js';
import igniteUiAceJava from './directives/ui-ace-java/ui-ace-java.directive';
import igniteUiAceSpring from './directives/ui-ace-spring/ui-ace-spring.directive';
import igniteUiAceCSharp from './directives/ui-ace-sharp/ui-ace-sharp.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAceTabs from './directives/ui-ace-tabs.directive';
import igniteRetainSelection from './directives/retain-selection.directive';
import btnIgniteLink from './directives/btn-ignite-link';

// Services.
import ChartColors from './services/ChartColors.service';
import Confirm from './services/Confirm.service.js';
import ConfirmBatch from './services/ConfirmBatch.service.js';
import CopyToClipboard from './services/CopyToClipboard.service';
import Countries from './services/Countries.service';
import ErrorPopover from './services/ErrorPopover.service';
import Focus from './services/Focus.service';
import FormUtils from './services/FormUtils.service';
import InetAddress from './services/InetAddress.service';
import JavaTypes from './services/JavaTypes.service';
import SqlTypes from './services/SqlTypes.service';
import LegacyTable from './services/LegacyTable.service';
import LegacyUtils from './services/LegacyUtils.service';
import Messages from './services/Messages.service';
import ModelNormalizer from './services/ModelNormalizer.service.js';
import UnsavedChangesGuard from './services/UnsavedChangesGuard.service';
import Clusters from './services/Clusters';
import Caches from './services/Caches';

import AngularStrapTooltip from './services/AngularStrapTooltip.decorator';
import AngularStrapSelect from './services/AngularStrapSelect.decorator';

// Filters.
import byName from './filters/byName.filter';
import defaultName from './filters/default-name.filter';
import domainsValidation from './filters/domainsValidation.filter';
import duration from './filters/duration.filter';
import hasPojo from './filters/hasPojo.filter';
import uiGridSubcategories from './filters/uiGridSubcategories.filter';
import id8 from './filters/id8.filter';

// Controllers
import profile from 'Controllers/profile-controller';
import resetPassword from './controllers/reset-password.controller';

// Components
import igniteListOfRegisteredUsers from './components/list-of-registered-users';
import IgniteActivitiesUserDialog from './components/activities-user-dialog';
import clusterSelect from './components/cluster-select';
import './components/input-dialog';
import webConsoleHeader from './components/web-console-header';
import webConsoleFooter from './components/web-console-footer';
import igniteIcon from './components/ignite-icon';
import versionPicker from './components/version-picker';
import userNotifications from './components/user-notifications';
import pageConfigure from './components/page-configure';
import pageConfigureBasic from './components/page-configure-basic';
import pageConfigureAdvanced from './components/page-configure-advanced';
import gridColumnSelector from './components/grid-column-selector';
import gridItemSelected from './components/grid-item-selected';
import bsSelectMenu from './components/bs-select-menu';
import protectFromBsSelectRender from './components/protect-from-bs-select-render';
import uiGridHovering from './components/ui-grid-hovering';
import listEditable from './components/list-editable';

import igniteServices from './services';

// Inject external modules.
import IgniteModules from 'IgniteModules/index';

import baseTemplate from 'views/base.pug';

angular.module('ignite-console', [
    // Optional AngularJS modules.
    'ngAnimate',
    'ngSanitize',
    // Third party libs.
    'btford.socket-io',
    'dndLists',
    'gridster',
    'mgcrea.ngStrap',
    'ngRetina',
    'nvd3',
    'pascalprecht.translate',
    'smart-table',
    'treeControl',
    'ui.grid',
    'ui.grid.autoResize',
    'ui.grid.exporter',
    'ui.grid.resizeColumns',
    'ui.grid.saveState',
    'ui.grid.selection',
    'ui.router',
    'ui.router.state.events',
    // Base modules.
    'ignite-console.core',
    'ignite-console.ace',
    'ignite-console.Form',
    'ignite-console.input-dialog',
    'ignite-console.user',
    'ignite-console.branding',
    'ignite-console.socket',
    'ignite-console.agent',
    'ignite-console.sql',
    'ignite-console.nodes',
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
    'ignite-console.loading',
    // Ignite configuration module.
    'ignite-console.config',
    // Components
    webConsoleHeader.name,
    webConsoleFooter.name,
    igniteIcon.name,
    igniteServices.name,
    versionPicker.name,
    userNotifications.name,
    pageConfigure.name,
    pageConfigureBasic.name,
    pageConfigureAdvanced.name,
    gridColumnSelector.name,
    gridItemSelected.name,
    bsSelectMenu.name,
    uiGridHovering.name,
    protectFromBsSelectRender.name,
    AngularStrapTooltip.name,
    AngularStrapSelect.name,
    listEditable.name,
    // Ignite modules.
    IgniteModules.name
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
.directive(...igniteUiAceSpring)
.directive(...igniteUiAceJava)
.directive(...igniteUiAceCSharp)
.directive(...igniteUiAcePojos)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAceTabs)
.directive(...igniteRetainSelection)
.directive('igniteOnFocusOut', igniteOnFocusOut)
.directive('igniteRestoreInputFocus', igniteRestoreInputFocus)
.directive('igniteListOfRegisteredUsers', igniteListOfRegisteredUsers)
.directive('igniteClusterSelect', clusterSelect)
.directive('btnIgniteLinkDashedSuccess', btnIgniteLink)
.directive('btnIgniteLinkDashedSecondary', btnIgniteLink)
// Services.
.service('IgniteErrorPopover', ErrorPopover)
.service('JavaTypes', JavaTypes)
.service('SqlTypes', SqlTypes)
.service(...ChartColors)
.service(...Confirm)
.service(...ConfirmBatch)
.service(...CopyToClipboard)
.service(...Countries)
.service(...Focus)
.service(...InetAddress)
.service(...Messages)
.service(...ModelNormalizer)
.service(...LegacyTable)
.service(...FormUtils)
.service(...LegacyUtils)
.service(...UnsavedChangesGuard)
.service('IgniteActivitiesUserDialog', IgniteActivitiesUserDialog)
.service('Clusters', Clusters)
.service('Caches', Caches)
// Controllers.
.controller(...resetPassword)
.controller(...profile)
// Filters.
.filter('byName', byName)
.filter('defaultName', defaultName)
.filter('domainsValidation', domainsValidation)
.filter('duration', duration)
.filter('hasPojo', hasPojo)
.filter('uiGridSubcategories', uiGridSubcategories)
.filter('id8', id8)
.config(['$translateProvider', '$stateProvider', '$locationProvider', '$urlRouterProvider', ($translateProvider, $stateProvider, $locationProvider, $urlRouterProvider) => {
    $translateProvider.translations('en', i18n);
    $translateProvider.preferredLanguage('en');

    // Set up the states.
    $stateProvider
        .state('base', {
            url: '',
            abstract: true,
            template: baseTemplate
        })
        .state('base.settings', {
            url: '/settings',
            abstract: true,
            template: '<ui-view></ui-view>'
        });

    $urlRouterProvider.otherwise('/404');
    $locationProvider.html5Mode(true);
}])
.run(['$rootScope', '$state', 'gettingStarted', ($root, $state, gettingStarted) => {
    $root._ = _;
    $root.$state = $state;
    $root.gettingStarted = gettingStarted;
}])
.run(['$rootScope', 'AgentManager', ($root, agentMgr) => {
    $root.$on('user', () => agentMgr.connect());
}])
.run(['$transitions', ($transitions) => {
    $transitions.onSuccess({ }, (trans) => {
        try {
            const {name, unsaved} = trans.$to();
            const params = trans.params();

            if (unsaved)
                localStorage.removeItem('lastStateChangeSuccess');
            else
                localStorage.setItem('lastStateChangeSuccess', JSON.stringify({name, params}));
        }
        catch (ignored) {
            // No-op.
        }
    });
}])
.run(['$rootScope', '$http', '$state', 'IgniteMessages', 'User', 'IgniteNotebookData',
    ($root, $http, $state, Messages, User, Notebook) => { // eslint-disable-line no-shadow
        $root.revertIdentity = () => {
            $http.get('/api/v1/admin/revert/identity')
                .then(() => User.load())
                .then((user) => {
                    $root.$broadcast('user', user);

                    $state.go('base.settings.admin');
                })
                .then(() => Notebook.load())
                .catch(Messages.showError);
        };
    }
]);
