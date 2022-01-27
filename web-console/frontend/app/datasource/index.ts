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
import angular from 'angular';
import uiValidate from 'angular-ui-validate';
import {UIRouterRx} from '@uirouter/rx';
import {UIRouter} from '@uirouter/angularjs';

import {withLatestFrom, tap, filter, scan} from 'rxjs/operators';



import ConfigureState from '../configuration/services/ConfigureState';
import PageConfigure from '../configuration/services/PageConfigure';
import ConfigSelectionManager from '../configuration/services/ConfigSelectionManager';


import itemsTable from '../configuration/components/pc-items-table';
import pcUiGridFilters from '../configuration/components/pc-ui-grid-filters';
import isInCollection from '../configuration/components/pcIsInCollection';
import pcValidation from '../configuration/components/pcValidation';

import pcSplitButton from '../configuration/components/pc-split-button';
import uiAceTabs from '../configuration/components/ui-ace-tabs.directive';

import pageDatasource from './components/page-datasource';
import pageDatasourceBasic from './components/page-datasource-basic';
import pageDatasourceAdvanced from './components/page-datasource-advanced';
import pageDatasourceOverview from './components/page-datasource-overview';

import selectors from './store/selectors';
import {registerStates} from './states';



import {errorState} from '../configuration/transitionHooks/errorState';
import {default as ActivitiesData} from '../core/activities/Activities.data';



registerActivitiesHook.$inject = ['$uiRouter', 'IgniteActivitiesData'];

function registerActivitiesHook($uiRouter: UIRouter, ActivitiesData: ActivitiesData) {
    $uiRouter.transitionService.onSuccess({to: 'base.datasource.**'}, (transition) => {
        ActivitiesData.post({group: 'datasource', action: transition.targetState().name()});
    });
}

export default angular
    .module('ignite-console.datasource', [
        uiValidate,
        'asyncFilter',
      
        pageDatasource.name,
        pageDatasourceBasic.name,
        pageDatasourceAdvanced.name,
        pageDatasourceOverview.name,
        pcUiGridFilters.name,
    
        itemsTable.name,
        pcValidation.name,
      
        pcSplitButton.name
      
    ])
    .config(registerStates)
    .run(registerActivitiesHook)
    .run(errorState)
    .run(['ConfigureState', '$uiRouter', (ConfigureState, $uiRouter) => {
        $uiRouter.plugin(UIRouterRx);

       


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
       
    }])
    .factory('configSelectionManager', ConfigSelectionManager)
      
    .service('PageConfigure', PageConfigure)
    .service('ConfigureState', ConfigureState)           
    .service('DatasourceSelectors', selectors)
    ;
