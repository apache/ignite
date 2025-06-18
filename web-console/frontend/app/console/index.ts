
import angular from 'angular';
import uiValidate from 'angular-ui-validate';
import {UIRouterRx} from '@uirouter/rx';
import {UIRouter} from '@uirouter/angularjs';
import ConfigureModule from '../configuration';

import Services from './services/Services';
import TaskFlows from './services/TaskFlows';

import pageConsole from './components/page-configure';
import pageConsoleBasic from './components/page-configure-basic';
import pageConsoleAdvanced from './components/page-configure-advanced';
import pageConsoleService from './components/page-console-service';
import pageConsoleCacheService from './components/page-console-cache-service';
import pageConsoleOverview from './components/page-configure-overview';

import modalImportService from './components/modal-import-service';
import buttonImportService from './components/button-import-service';


import pcItemsTable from '../configuration/components/pc-items-table';
import pcUiGridFilters from '../configuration/components/pc-ui-grid-filters';
import pcValidation from '../configuration/components/pcValidation';
import pcSplitButton from '../configuration/components/pc-split-button';


import {registerStates} from './states';


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
        ConfigureModule.name,
      
        pageConsole.name,
        pageConsoleBasic.name,
        pageConsoleAdvanced.name,
        pageConsoleService.name,
        pageConsoleCacheService.name,
        pageConsoleOverview.name,
        

        modalImportService.name,
        buttonImportService.name,
        
        pcUiGridFilters.name,
        pcItemsTable.name,
        pcValidation.name,      
        pcSplitButton.name
      
    ])
    .config(registerStates)
    .run(registerActivitiesHook)
    .run(errorState)

    .service('Services', Services)
    .service('TaskFlows', TaskFlows)
    ;
