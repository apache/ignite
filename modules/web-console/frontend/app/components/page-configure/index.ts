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

import 'angular1-async-filter';
import {UIRouterRx} from '@uirouter/rx';
import uiValidate from 'angular-ui-validate';

import component from './component';
import ConfigureState from './services/ConfigureState';
import PageConfigure from './services/PageConfigure';
import ConfigurationDownload from './services/ConfigurationDownload';
import ConfigChangesGuard from './services/ConfigChangesGuard';
import ConfigSelectionManager from './services/ConfigSelectionManager';
import SummaryZipper from './services/SummaryZipper';
import ConfigurationResource from './services/ConfigurationResource';
import selectors from './store/selectors';
import effects from './store/effects';

import projectStructurePreview from './components/modal-preview-project';
import itemsTable from './components/pc-items-table';
import pcUiGridFilters from './components/pc-ui-grid-filters';
import isInCollection from './components/pcIsInCollection';
import pcValidation from './components/pcValidation';
import fakeUiCanExit from './components/fakeUICanExit';
import formUICanExitGuard from './components/formUICanExitGuard';
import modalImportModels from './components/modal-import-models';
import buttonImportModels from './components/button-import-models';
import buttonDownloadProject from './components/button-download-project';
import buttonPreviewProject from './components/button-preview-project';
import previewPanel from './components/preview-panel';
import pcSplitButton from './components/pc-split-button';

import {errorState} from './transitionHooks/errorState';
import {default as ActivitiesData} from 'app/core/activities/Activities.data';

import {withLatestFrom, tap, filter, scan} from 'rxjs/operators';

import {navigationMenuItem, AppStore} from '../../store';
import {default as configurationIcon} from './icons/configuration.icon.svg';
import {default as IconsService} from '../ignite-icon/service';

import {
    editReducer2,
    shortObjectsReducer,
    editReducer,
    loadingReducer,
    itemsEditReducerFactory,
    mapStoreReducerFactory,
    mapCacheReducerFactory,
    basicCachesActionTypes,
    clustersActionTypes,
    shortClustersActionTypes,
    cachesActionTypes,
    shortCachesActionTypes,
    modelsActionTypes,
    shortModelsActionTypes,
    igfssActionTypes,
    shortIGFSsActionTypes,
    refsReducer
} from './reducer';

import {registerStates} from './states';

/**
 * @param {uirouter.UIRouter} $uiRouter
 * @param {ActivitiesData} ActivitiesData
 */
function registerActivitiesHook($uiRouter, ActivitiesData) {
    $uiRouter.transitionService.onSuccess({to: 'base.configuration.**'}, (transition) => {
        ActivitiesData.post({group: 'configuration', action: transition.targetState().name()});
    });
}

registerActivitiesHook.$inject = ['$uiRouter', 'IgniteActivitiesData'];

export default angular
    .module('ignite-console.page-configure', [
        'ui.router',
        'asyncFilter',
        uiValidate,
        pcUiGridFilters.name,
        projectStructurePreview.name,
        itemsTable.name,
        pcValidation.name,
        modalImportModels.name,
        buttonImportModels.name,
        buttonDownloadProject.name,
        buttonPreviewProject.name,
        previewPanel.name,
        pcSplitButton.name
    ])
    .config(registerStates)
    .config(['DefaultStateProvider', (DefaultState) => {
        DefaultState.setRedirectTo(() => 'base.configuration.overview');
    }])
    .run(registerActivitiesHook)
    .run(['ConfigEffects', 'ConfigureState', '$uiRouter', 'Store', 'IgniteIcon', (ConfigEffects, ConfigureState, $uiRouter, store: AppStore, icons: IconsService) => {
        icons.registerIcons({configuration: configurationIcon});

        $uiRouter.plugin(UIRouterRx);

        ConfigureState.addReducer(refsReducer({
            models: {at: 'domains', store: 'caches'},
            caches: {at: 'caches', store: 'models'}
        }));

        ConfigureState.addReducer((state, action) => Object.assign({}, state, {
            clusterConfiguration: editReducer(state.clusterConfiguration, action),
            configurationLoading: loadingReducer(state.configurationLoading, action),
            basicCaches: itemsEditReducerFactory(basicCachesActionTypes)(state.basicCaches, action),
            clusters: mapStoreReducerFactory(clustersActionTypes)(state.clusters, action),
            shortClusters: mapCacheReducerFactory(shortClustersActionTypes)(state.shortClusters, action),
            caches: mapStoreReducerFactory(cachesActionTypes)(state.caches, action),
            shortCaches: mapCacheReducerFactory(shortCachesActionTypes)(state.shortCaches, action),
            models: mapStoreReducerFactory(modelsActionTypes)(state.models, action),
            shortModels: mapCacheReducerFactory(shortModelsActionTypes)(state.shortModels, action),
            igfss: mapStoreReducerFactory(igfssActionTypes)(state.igfss, action),
            shortIgfss: mapCacheReducerFactory(shortIGFSsActionTypes)(state.shortIgfss, action),
            edit: editReducer2(state.edit, action)
        }));

        ConfigureState.addReducer(shortObjectsReducer);

        ConfigureState.addReducer((state, action) => {
            switch (action.type) {
                case 'APPLY_ACTIONS_UNDO':
                    return action.state;

                default:
                    return state;
            }
        });

        const la = ConfigureState.actions$.pipe(scan((acc, action) => [...acc, action], []));

        ConfigureState.actions$.pipe(
            filter((a) => a.type === 'UNDO_ACTIONS'),
            withLatestFrom(la, ({actions}, actionsWindow, initialState) => {
                return {
                    type: 'APPLY_ACTIONS_UNDO',
                    state: actionsWindow.filter((a) => !actions.includes(a)).reduce(ConfigureState._combinedReducer, {})
                };
            }),
            tap((a) => ConfigureState.dispatchAction(a))
        )
        .subscribe();
        ConfigEffects.connect();

        store.dispatch(navigationMenuItem({
            activeSref: 'base.configuration.**',
            icon: 'configuration',
            label: 'Configuration',
            order: 1,
            sref: 'base.configuration.overview'
        }));
    }])
    .component('pageConfigure', component)
    .directive('pcIsInCollection', isInCollection)
    .directive('fakeUiCanExit', fakeUiCanExit)
    .directive('formUiCanExitGuard', formUICanExitGuard)
    .factory('configSelectionManager', ConfigSelectionManager)
    .service('IgniteSummaryZipper', SummaryZipper)
    .service('IgniteConfigurationResource', ConfigurationResource)
    .service('ConfigSelectors', selectors)
    .service('ConfigEffects', effects)
    .service('ConfigChangesGuard', ConfigChangesGuard)
    .service('PageConfigure', PageConfigure)
    .service('ConfigureState', ConfigureState)
    .service('ConfigurationDownload', ConfigurationDownload)
    .run(errorState);
