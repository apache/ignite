

import './style.scss';

import angular from 'angular';
import {StateRegistry, UIRouter} from '@uirouter/angularjs';

import queriesNotebooksList from './components/queries-notebooks-list';
import queriesNotebook from './components/queries-notebook';
import pageQueriesCmp from './component';
import {default as ActivitiesData} from 'app/core/activities/Activities.data';
import Notebook from './notebook.service';
import {navigationMenuItem, AppStore} from '../../store';

function registerActivitiesHook($uiRouter: UIRouter, ActivitiesData: ActivitiesData) {
    $uiRouter.transitionService.onSuccess({to: 'base.sql.**'}, (transition) => {
        ActivitiesData.post({group: 'sql', action: transition.targetState().name()});
    });
}

registerActivitiesHook.$inject = ['$uiRouter', 'IgniteActivitiesData'];

export default angular.module('ignite-console.sql', [
    'ui.router',
    queriesNotebooksList.name,
    queriesNotebook.name
])
    .run(['Store', '$translate', (store: AppStore, $translate: ng.translate.ITranslateService) => {
        store.dispatch(navigationMenuItem({
            activeSref: 'base.sql.**',
            icon: 'sql',
            label: $translate.instant('navigationMenu.queriesLinkLabel'),
            order: 2,
            sref: 'base.sql.tabs.notebooks-list'
        }));
    }])
    .component('pageQueries', pageQueriesCmp)
    .component('pageQueriesSlot', {
        require: {
            pageQueries: '^pageQueries'
        },
        bindings: {
            slotName: '<'
        },
        controller: class {
            static $inject = ['$transclude', '$timeout'];

            constructor($transclude, $timeout) {
                this.$transclude = $transclude;
                this.$timeout = $timeout;
            }

            $postLink() {
                this.$transclude((clone) => {
                    this.pageQueries[this.slotName].empty();
                    clone.appendTo(this.pageQueries[this.slotName]);
                });
            }
        },
        transclude: true
    })
    .service('IgniteNotebook', Notebook)
    .run(['$stateRegistry', '$translate', ($stateRegistry: StateRegistry, $translate: ng.translate.ITranslateService) => {
        // set up the states
        $stateRegistry.register({
            name: 'base.sql',
            abstract: true
        });
        $stateRegistry.register({
            name: 'base.sql.tabs',
            url: '/queries',
            component: 'pageQueries',
            redirectTo: 'base.sql.tabs.notebooks-list',
            permission: 'query'
        });
        $stateRegistry.register({
            name: 'base.sql.tabs.notebooks-list',
            url: '/notebooks',
            component: 'queriesNotebooksList',
            permission: 'query',
            tfMetaTags: {
                title: $translate.instant('queries.notebooks.documentTitle')
            }
        });
        $stateRegistry.register({
            name: 'base.sql.notebook',
            url: '/notebook/{noteId}',
            component: 'queriesNotebook',
            permission: 'query',
            tfMetaTags: {
                title: $translate.instant('queries.notebook.documentTitle')
            }
        });
    }])
    .run(registerActivitiesHook);
