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

import templateUrl from './dropdown.jade';

export default ['igniteFormFieldDropdown', ['IgniteFormGUID', 'IgniteLegacyTable', (guid, LegacyTable) => {
    const controller = () => {};

    const link = (scope, $element, attrs, [form, label]) => {
        const {id, name} = scope;

        scope.id = id || guid();

        if (label) {
            label.for = scope.id;

            scope.label = label;

            scope.$watch('required', (required) => {
                label.required = required || false;
            });
        }

        form.$defaults = form.$defaults || {};
        form.$defaults[name] = _.cloneDeep(scope.value);

        const setAsDefault = () => {
            if (!form.$pristine) return;

            form.$defaults = form.$defaults || {};
            form.$defaults[name] = _.cloneDeep(scope.value);
        };

        scope.$watch(() => form.$pristine, setAsDefault);
        scope.$watch('value', setAsDefault);

        scope.tableReset = () => {
            LegacyTable.tableSaveAndReset();
        };
    };

    return {
        restrict: 'E',
        scope: {
            id: '@',
            name: '@',
            required: '=ngRequired',
            value: '=ngModel',

            focus: '=ngFocus',

            onEnter: '@'
        },
        bindToController: {
            value: '=ngModel',
            placeholder: '@',
            options: '=',
            ngDisabled: '=',
            multiple: '='
        },
        link,
        templateUrl,
        controller,
        controllerAs: 'dropdown',
        replace: true,
        transclude: true,
        require: ['^form', '?^igniteFormField']
    };
}]];
