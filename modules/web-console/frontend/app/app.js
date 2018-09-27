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

import './style.scss';

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
import igniteAutoFocus from './directives/auto-focus.directive';
import igniteBsAffixUpdate from './directives/bs-affix-update.directive';
import igniteCentered from './directives/centered/centered.directive';
import igniteCopyToClipboard from './directives/copy-to-clipboard.directive';
import igniteHideOnStateChange from './directives/hide-on-state-change/hide-on-state-change.directive';
import igniteInformation from './directives/information/information.directive';
import igniteMatch from './directives/match.directive';
import igniteOnClickFocus from './directives/on-click-focus.directive';
import igniteOnEnter from './directives/on-enter.directive';
import igniteOnEnterFocusMove from './directives/on-enter-focus-move.directive';
import igniteOnEscape from './directives/on-escape.directive';
import igniteOnFocusOut from './directives/on-focus-out.directive';
import igniteRestoreInputFocus from './directives/restore-input-focus.directive';
import igniteUiAceCSharp from './directives/ui-ace-sharp/ui-ace-sharp.directive';
import igniteUiAcePojos from './directives/ui-ace-pojos/ui-ace-pojos.directive';
import igniteUiAcePom from './directives/ui-ace-pom/ui-ace-pom.directive';
import igniteUiAceDocker from './directives/ui-ace-docker/ui-ace-docker.directive';
import igniteUiAceTabs from './directives/ui-ace-tabs.directive';
import igniteRetainSelection from './directives/retain-selection.directive';
import btnIgniteLink from './directives/btn-ignite-link';

// Services.
import ChartColors from './services/ChartColors.service';
import {default as IgniteConfirm, Confirm} from './services/Confirm.service';
import ConfirmBatch from './services/ConfirmBatch.service';
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
import ModelNormalizer from './services/ModelNormalizer.service';
import Caches from './services/Caches';
import {CSV} from './services/CSV';
import {$exceptionHandler} from './services/exceptionHandler';
import IGFSs from './services/IGFSs';
import Models from './services/Models';

import AngularStrapTooltip from './services/AngularStrapTooltip.decorator';
import AngularStrapSelect from './services/AngularStrapSelect.decorator';

// Filters.
import byName from './filters/byName.filter';
import bytes from './filters/bytes.filter';
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
import gridShowingRows from './components/grid-showing-rows';
import bsSelectMenu from './components/bs-select-menu';
import protectFromBsSelectRender from './components/protect-from-bs-select-render';
import uiGrid from './components/ui-grid';
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
import igniteChart from './components/ignite-chart';
import igniteChartSelector from './components/ignite-chart-series-selector';
import igniteStatus from './components/ignite-status';

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
    gridShowingRows.name,
    bsSelectMenu.name,
    uiGrid.name,
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
    igniteChart.name,
    igniteChartSelector.name,
    igniteStatus.name,
    progressLine.name,
    formField.name
])
.service('$exceptionHandler', $exceptionHandler)
// Directives.
.directive('igniteAutoFocus', igniteAutoFocus)
.directive('igniteBsAffixUpdate', igniteBsAffixUpdate)
.directive('centered', igniteCentered)
.directive('igniteCopyToClipboard', igniteCopyToClipboard)
.directive('hideOnStateChange', igniteHideOnStateChange)
.directive('igniteInformation', igniteInformation)
.directive('igniteMatch', igniteMatch)
.directive('igniteOnClickFocus', igniteOnClickFocus)
.directive('igniteOnEnter', igniteOnEnter)
.directive('igniteOnEnterFocusMove', igniteOnEnterFocusMove)
.directive('igniteOnEscape', igniteOnEscape)
.directive('igniteUiAceSharp', igniteUiAceCSharp)
.directive('igniteUiAcePojos', igniteUiAcePojos)
.directive('igniteUiAcePom', igniteUiAcePom)
.directive('igniteUiAceDocker', igniteUiAceDocker)
.directive('igniteUiAceTabs', igniteUiAceTabs)
.directive('igniteRetainSelection', igniteRetainSelection)
.directive('igniteOnFocusOut', igniteOnFocusOut)
.directive('igniteRestoreInputFocus', igniteRestoreInputFocus)
.directive('btnIgniteLinkDashedSuccess', btnIgniteLink)
.directive('btnIgniteLinkDashedSecondary', btnIgniteLink)
// Services.
.service('IgniteErrorPopover', ErrorPopover)
.service('JavaTypes', JavaTypes)
.service('SqlTypes', SqlTypes)
.service('IgniteChartColors', ChartColors)
.service('IgniteConfirm', IgniteConfirm)
.service('Confirm', Confirm)
.service('IgniteConfirmBatch', ConfirmBatch)
.service('IgniteCopyToClipboard', CopyToClipboard)
.service('IgniteCountries', Countries)
.service('IgniteFocus', Focus)
.service('IgniteInetAddress', InetAddress)
.service('IgniteMessages', Messages)
.service('IgniteErrorParser', ErrorParser)
.service('IgniteModelNormalizer', ModelNormalizer)
.service('IgniteLegacyTable', LegacyTable)
.service('IgniteFormUtils', FormUtils)
.service('IgniteLegacyUtils', LegacyUtils)
.service('IgniteActivitiesUserDialog', IgniteActivitiesUserDialog)
.service('Caches', Caches)
.service('CSV', CSV)
.service('IGFSs', IGFSs)
.service('Models', Models)
// Filters.
.filter('byName', byName)
.filter('bytes', bytes)
.filter('defaultName', defaultName)
.filter('domainsValidation', domainsValidation)
.filter('duration', duration)
.filter('hasPojo', hasPojo)
.filter('uiGridSubcategories', uiGridSubcategories)
.filter('id8', id8)
.config(['$translateProvider', '$stateProvider', '$locationProvider', '$urlRouterProvider',
    /**
     * @param {angular.translate.ITranslateProvider} $translateProvider
     * @param {import('@uirouter/angularjs').StateProvider} $stateProvider
     * @param {ng.ILocationProvider} $locationProvider
     * @param {import('@uirouter/angularjs').UrlRouterProvider} $urlRouterProvider
     */
    ($translateProvider, $stateProvider, $locationProvider, $urlRouterProvider) => {
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
.run(['$rootScope', '$state', 'gettingStarted',
    /**
     * @param {ng.IRootScopeService} $root
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {ReturnType<typeof import('./modules/getting-started/GettingStarted.provider').service>} gettingStarted
     */
    ($root, $state, gettingStarted) => {
        $root._ = _;
        $root.$state = $state;
        $root.gettingStarted = gettingStarted;
    }
])
.run(['$rootScope', 'AgentManager',
    /**
     * @param {ng.IRootScopeService} $root
     * @param {import('./modules/agent/AgentManager.service').default} agentMgr
     */
    ($root, agentMgr) => {
        let lastUser;

        $root.$on('user', (e, user) => {
            if (lastUser)
                return;

            lastUser = user;

            agentMgr.connect();
        });
    }
])
.run(['$transitions',
    /**
     * @param {import('@uirouter/angularjs').TransitionService} $transitions
     */
    ($transitions) => {
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
    }
])
.run(['$rootScope', '$http', '$state', 'IgniteMessages', 'User', 'IgniteNotebookData',
    /**
     * @param {ng.IRootScopeService} $root
     * @param {ng.IHttpService} $http
     * @param {ReturnType<typeof import('./services/Messages.service').default>} Messages
     */
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
.run(['IgniteIcon',
    /**
     * @param {import('./components/ignite-icon/service').default} IgniteIcon
     */
    (IgniteIcon) => IgniteIcon.registerIcons(icons)
]);
