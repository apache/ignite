
import angular from 'angular';
import uiValidate from 'angular-ui-validate';
import {UIRouterRx} from '@uirouter/rx';
import {UIRouter} from '@uirouter/angularjs';

import {withLatestFrom, tap, filter, scan} from 'rxjs/operators';



import ConfigureState from '../configuration/services/ConfigureState';
import PageConfigure from '../configuration/services/PageConfigure';
import ConfigurationDownload from '../configuration/services/ConfigurationDownload';
import ConfigSelectionManager from '../configuration/services/ConfigSelectionManager';
import SummaryZipper from '../configuration/services/SummaryZipper';
import ConfigurationResource from '../configuration/services/ConfigurationResource';
import selectors from '../configuration/store/selectors';
import effects from '../configuration/store/effects';
import Clusters from '../configuration/services/Clusters';
import Caches from '../configuration/services/Caches';
import Models from '../configuration/services/Models';
import Services from './services/Services';
import TaskFlows from './services/TaskFlows';

import pageConsole from './components/page-configure';
import pageConsoleBasic from './components/page-configure-basic';
import pageConsoleAdvanced from './components/page-configure-advanced';
import pageConsoleService from './components/page-console-service';
import pageConsoleCacheService from './components/page-console-cache-service';
import pageConsoleOverview from './components/page-configure-overview';


import itemsTable from '../configuration/components/pc-items-table';
import pcUiGridFilters from '../configuration/components/pc-ui-grid-filters';

import pcValidation from '../configuration/components/pcValidation';
import pcSplitButton from '../configuration/components/pc-split-button';


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
} from 'app/configuration/store/reducer';

import {errorState} from '../configuration/transitionHooks/errorState';
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
        pageConsoleCacheService.name,
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
    .service('IgniteSummaryZipper', SummaryZipper)
    .service('IgniteConfigurationResource', ConfigurationResource)
    .service('ConfigSelectors', selectors)
    .service('ConfigEffects', effects)    
    .service('PageConfigure', PageConfigure)
    .service('ConfigureState', ConfigureState)    
    .service('ConfigurationDownload', ConfigurationDownload)
    .service('Clusters', Clusters)
    .service('Caches', Caches) 
    .service('Models', Models) 
    .service('Services', Services)
    .service('TaskFlows', TaskFlows)
    ;
