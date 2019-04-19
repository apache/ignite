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
