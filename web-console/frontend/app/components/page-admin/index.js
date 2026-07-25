

import './style.scss';

import controller from './controller';
import templateUrl from './template.tpl.pug';
import {default as ActivitiesData} from 'app/core/activities/Activities.data';

/**
 * @param {import('@uirouter/angularjs').UIRouter} $uiRouter
 * @param {ActivitiesData} ActivitiesData
 */
function registerActivitiesHook($uiRouter, ActivitiesData) {
    $uiRouter.transitionService.onSuccess({to: 'base.settings.**'}, (transition) => {
        ActivitiesData.post({group: 'settings', action: transition.targetState().name()});
    });
}

registerActivitiesHook.$inject = ['$uiRouter', 'IgniteActivitiesData'];

export default angular
    .module('ignite-console.page-admin', [])
    .component('pageAdmin', {
        controller,
        templateUrl
    })
    .run(registerActivitiesHook);
