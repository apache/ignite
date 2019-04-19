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

import template from './ui-ace-spring.pug';
import IgniteUiAceSpring from './ui-ace-spring.controller';

export default function() {
    return {
        priority: 1,
        restrict: 'E',
        scope: {
            master: '=',
            detail: '='
        },
        bindToController: {
            data: '=?ngModel',
            generator: '@',
            client: '@'
        },
        template,
        controller: IgniteUiAceSpring,
        controllerAs: 'ctrl',
        require: {
            ctrl: 'igniteUiAceSpring',
            igniteUiAceTabs: '?^igniteUiAceTabs',
            formCtrl: '?^form',
            ngModelCtrl: '?ngModel'
        }
    };
}
