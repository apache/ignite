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

import './vendor';
import '../public/stylesheets/style.scss';
import '../app/primitives';

import './app.config';

import './modules/form/form.module';
import './modules/agent/agent.module';
import './modules/nodes/nodes.module';
import './modules/demo/Demo.module';

import './modules/states/logout.state';
import './modules/states/admin.state';
import './modules/states/errors.state';
import './modules/states/settings.state';

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
import servicesModule from './services';
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
import igniteUiAceCSharp from './directives/ui-ace-sharp/ui-ace-sharp.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAceTabs from './directives/ui-ace-tabs.directive';
import igniteRetainSelection from './directives/retain-selection.directive';
import btnIgniteLink from './directives/btn-ignite-link';
import exposeInput from './components/expose-ignite-form-field-control';

// Services.
import ChartColors from './services/ChartColors.service';
import {default as IgniteConfirm, Confirm} from './services/Confirm.service.js';
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
import ErrorParser from './services/ErrorParser.service';
import ModelNormalizer from './services/ModelNormalizer.service.js';
import UnsavedChangesGuard from './services/UnsavedChangesGuard.service';
import Caches from './services/Caches';
import {CSV} from './services/CSV';
import {$exceptionHandler} from './services/exceptionHandler.js';
import IGFSs from './services/IGFSs';
import Models from './services/Models';

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

// Components
import igniteListOfRegisteredUsers from './components/list-of-registered-users';
import IgniteActivitiesUserDialog from './components/activities-user-dialog';
import './components/input-dialog';
import webConsoleHeader from './components/web-console-header';
import webConsoleFooter from './components/web-console-footer';
import igniteIcon from './components/ignite-icon';
import versionPicker from './components/version-picker';
import userNotifications from './components/user-notifications';
import pageAdmin from './components/page-admin';
import pageConfigure from './components/page-configure';
import pageConfigureBasic from './components/page-configure-basic';
import pageConfigureAdvanced from './components/page-configure-advanced';
import pageQueries from './components/page-queries';
import pageConfigureOverview from './components/page-configure-overview';
import gridColumnSelector from './components/grid-column-selector';
import gridItemSelected from './components/grid-item-selected';
import gridNoData from './components/grid-no-data';
import gridExport from './components/grid-export';
import bsSelectMenu from './components/bs-select-menu';
import protectFromBsSelectRender from './components/protect-from-bs-select-render';
import uiGridHovering from './components/ui-grid-hovering';
import uiGridFilters from './components/ui-grid-filters';
import uiGridColumnResizer from './components/ui-grid-column-resizer';
import listEditable from './components/list-editable';
import breadcrumbs from './components/breadcrumbs';
import panelCollapsible from './components/panel-collapsible';
import clusterSelector from './components/cluster-selector';
import connectedClusters from './components/connected-clusters-badge';
import connectedClustersDialog from './components/connected-clusters-dialog';
import pageLanding from './components/page-landing';
import passwordVisibility from './components/password-visibility';
import progressLine from './components/progress-line';
import formField from './components/form-field';

import pageProfile from './components/page-profile';
import pagePasswordChanged from './components/page-password-changed';
import pagePasswordReset from './components/page-password-reset';
import pageSignup from './components/page-signup';
import pageSignin from './components/page-signin';
import pageForgotPassword from './components/page-forgot-password';

import igniteServices from './services';

import uiAceJava from './directives/ui-ace-java';
import uiAceSpring from './directives/ui-ace-spring';

import baseTemplate from 'views/base.pug';
import * as icons from '../public/images/icons';

export default angular.module('ignite-console', [
    // Optional AngularJS modules.
    'ngAnimate',
    'ngSanitize',
    'ngMessages',
    // Third party libs.
    'btford.socket-io',
    'dndLists',
    'gridster',
    'mgcrea.ngStrap',
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
    'ui.carousel',
    // Base modules.
    'ignite-console.core',
    'ignite-console.ace',
    'ignite-console.Form',
    'ignite-console.input-dialog',
    'ignite-console.user',
    'ignite-console.branding',
    'ignite-console.socket',
    'ignite-console.agent',
    'ignite-console.nodes',
    'ignite-console.demo',
    // States.
    'ignite-console.states.logout',
    'ignite-console.states.admin',
    'ignite-console.states.errors',
    'ignite-console.states.settings',
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
    pageAdmin.name,
    pageConfigure.name,
    pageConfigureBasic.name,
    pageConfigureAdvanced.name,
    pageQueries.name,
    pageConfigureOverview.name,
    gridColumnSelector.name,
    gridItemSelected.name,
    gridNoData.name,
    gridExport.name,
    bsSelectMenu.name,
    uiGridHovering.name,
    uiGridFilters.name,
    uiGridColumnResizer.name,
    protectFromBsSelectRender.name,
    AngularStrapTooltip.name,
    AngularStrapSelect.name,
    listEditable.name,
    panelCollapsible.name,
    clusterSelector.name,
    servicesModule.name,
    connectedClusters.name,
    connectedClustersDialog.name,
    igniteListOfRegisteredUsers.name,
    pageProfile.name,
    exposeInput.name,
    pageLanding.name,
    pagePasswordChanged.name,
    pagePasswordReset.name,
    pageSignup.name,
    pageSignin.name,
    pageForgotPassword.name,
    uiAceJava.name,
    uiAceSpring.name,
    breadcrumbs.name,
    passwordVisibility.name,
    progressLine.name,
    formField.name
])
.service($exceptionHandler.name, $exceptionHandler)
// Directives.
.directive(...igniteAutoFocus)
.directive(...igniteBsAffixUpdate)
.directive(...igniteCentered)
.directive(...igniteCopyToClipboard)
.directive(...igniteHideOnStateChange)
.directive(...igniteInformation)
.directive('igniteMatch', igniteMatch)
.directive(...igniteOnClickFocus)
.directive(...igniteOnEnter)
.directive(...igniteOnEnterFocusMove)
.directive(...igniteOnEscape)
.directive(...igniteUiAceCSharp)
.directive(...igniteUiAcePojos)
.directive(...igniteUiAcePom)
.directive(...igniteUiAceDocker)
.directive(...igniteUiAceTabs)
.directive(...igniteRetainSelection)
.directive('igniteOnFocusOut', igniteOnFocusOut)
.directive('igniteRestoreInputFocus', igniteRestoreInputFocus)
.directive('btnIgniteLinkDashedSuccess', btnIgniteLink)
.directive('btnIgniteLinkDashedSecondary', btnIgniteLink)
// Services.
.service('IgniteErrorPopover', ErrorPopover)
.service('JavaTypes', JavaTypes)
.service('SqlTypes', SqlTypes)
.service(...ChartColors)
.service(...IgniteConfirm)
.service(Confirm.name, Confirm)
.service('IgniteConfirmBatch', ConfirmBatch)
.service(...CopyToClipboard)
.service(...Countries)
.service(...Focus)
.service(...InetAddress)
.service(...Messages)
.service('IgniteErrorParser', ErrorParser)
.service(...ModelNormalizer)
.service(...LegacyTable)
.service(...FormUtils)
.service(...LegacyUtils)
.service(...UnsavedChangesGuard)
.service('IgniteActivitiesUserDialog', IgniteActivitiesUserDialog)
.service('Caches', Caches)
.service(CSV.name, CSV)
.service('IGFSs', IGFSs)
.service('Models', Models)
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
    let lastUser;

    $root.$on('user', (e, user) => {
        if (lastUser)
            return;

        lastUser = user;

        agentMgr.connect();
    });
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
                .then(() => $state.go('base.settings.admin'))
                .then(() => Notebook.load())
                .catch(Messages.showError);
        };
    }
])
.run(['IgniteIcon', (IgniteIcon) => IgniteIcon.registerIcons(icons)]);
