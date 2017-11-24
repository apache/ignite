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

import templateUrl from './ui-ace-java.jade';
import controller from './ui-ace-java.controller';

export default ['igniteUiAceJava', [() => {
    const link = (scope, $el, attrs, [ctrl, igniteUiAceTabs, formCtrl, ngModelCtrl]) => {
        if (formCtrl && ngModelCtrl)
            formCtrl.$removeControl(ngModelCtrl);

        if (igniteUiAceTabs && igniteUiAceTabs.onLoad) {
            scope.onLoad = (editor) => {
                igniteUiAceTabs.onLoad(editor);

                scope.$watch('master', () => editor.attractAttention = false);
            };
        }

        if (igniteUiAceTabs && igniteUiAceTabs.onChange)
            scope.onChange = igniteUiAceTabs.onChange;

        const noDeepWatch = !(typeof attrs.noDeepWatch !== 'undefined');

        // Setup watchers.
        scope.$watch('master', () => {
            ctrl.data = _.isNil(scope.master) ? null : ctrl.generate(scope.master, scope.detail).asString();
        }, noDeepWatch);
    };

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
        link,
        templateUrl,
        controller,
        controllerAs: 'ctrl',
        require: ['igniteUiAceJava', '?^igniteUiAceTabs', '?^form', '?ngModel']
    };
}]];
