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
