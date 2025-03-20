
import angular from 'angular';
import uiValidate from 'angular-ui-validate';
import {UIRouterRx} from '@uirouter/rx';
import {UIRouter} from '@uirouter/angularjs';

import {withLatestFrom, tap, filter, scan} from 'rxjs/operators';

import ConfigureState from '../configuration/services/ConfigureState';
import ConfigSelectionManager from '../configuration/services/ConfigSelectionManager';


import itemsTable from '../configuration/components/pc-items-table';
import pcUiGridFilters from '../configuration/components/pc-ui-grid-filters';
import isInCollection from '../configuration/components/pcIsInCollection';
import pcValidation from '../configuration/components/pcValidation';
import pcSplitButton from '../configuration/components/pc-split-button';

import pageDatasets from './components/page-datasets';
import pageDatasetsOverview from './components/page-datasets-overview';
import pageDatasetsAdvanced from './components/page-datasets-advanced';
import pageChinaMap from './components/page-datasets-china-map';

import {registerStates} from './states';
import {errorState} from '../configuration/transitionHooks/errorState';



export default angular
    .module('ignite-console.datasets', [
        'ngSanitize',
        uiValidate,
        'asyncFilter', 
        
        pageDatasets.name,
        pageDatasetsOverview.name,
        pageDatasetsAdvanced.name,
        pageChinaMap.name,

        pcUiGridFilters.name,    
        itemsTable.name,
        pcValidation.name,      
        pcSplitButton.name
      
    ])
    .config(registerStates)
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
    .service('ConfigureState', ConfigureState)           

    ;
