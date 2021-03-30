/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import uiValidate from 'angular-ui-validate';
import {UIRouterRx} from '@uirouter/rx';
import {UIRouter} from '@uirouter/angularjs';

import {withLatestFrom, tap, filter, scan} from 'rxjs/operators';



import ConfigureState from './services/ConfigureState';
import PageConfigure from './services/PageConfigure';
import ConfigSelectionManager from './services/ConfigSelectionManager';

import ConfigurationResource from './services/ConfigurationResource';
import selectors from './store/selectors';
import effects from './store/effects';
import Clusters from './services/Clusters';
import Caches from './services/Caches';
import Models from './services/Models';
import Services from './services/Services';

import pageConsole from './components/page-configure';
import pageConsoleBasic from './components/page-configure-basic';
import pageConsoleAdvanced from './components/page-configure-advanced';
import pageConsoleService from './components/page-console-service';
import pageConsoleOverview from './components/page-configure-overview';


import itemsTable from '../configuration/components/pc-items-table';
import pcUiGridFilters from '../configuration/components/pc-ui-grid-filters';
import isInCollection from '../configuration/components/pcIsInCollection';
import pcValidation from '../configuration/components/pcValidation';

import pcSplitButton from '../configuration/components/pc-split-button';
import uiAceTabs from '../configuration/components/ui-ace-tabs.directive';



import {registerStates} from './states';

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
    refsReducer
} from './store/reducer';

import {errorState} from './transitionHooks/errorState';
import {default as ActivitiesData} from '../core/activities/Activities.data';



registerActivitiesHook.$inject = ['$uiRouter', 'IgniteActivitiesData'];

function registerActivitiesHook($uiRouter: UIRouter, ActivitiesData: ActivitiesData) {
    $uiRouter.transitionService.onSuccess({to: 'base.console.**'}, (transition) => {
        ActivitiesData.post({group: 'console', action: transition.targetState().name()});
    });
}

export default angular
    .module('ignite-console.console', [
        uiValidate,
        'asyncFilter',
      
        pageConsole.name,
        pageConsoleBasic.name,
        pageConsoleAdvanced.name,
        pageConsoleService.name,
        pageConsoleOverview.name,
        pcUiGridFilters.name,
    
        itemsTable.name,
        pcValidation.name,
      
        pcSplitButton.name
      
    ])
    .config(registerStates)
    .run(registerActivitiesHook)
    .run(errorState)
    .run(['ConfigEffects', 'ConfigureState', '$uiRouter', (ConfigEffects, ConfigureState, $uiRouter) => {
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
    }])
    .factory('configSelectionManager', ConfigSelectionManager)
    
    .service('IgniteConfigurationResource', ConfigurationResource)
    .service('ConfigSelectors', selectors)
    .service('ConfigEffects', effects)    
    .service('PageConfigure', PageConfigure)
    .service('ConfigureState', ConfigureState)    
    .service('Clusters', Clusters)
    .service('Caches', Caches) 
    .service('Models', Models) 
    .service('Services', Services)       
    //.directive('pcIsInCollection', isInCollection)    
    //.directive('igniteUiAceTabs', uiAceTabs)
    ;
