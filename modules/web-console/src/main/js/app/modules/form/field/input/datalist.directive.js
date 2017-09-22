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

import templateUrl from './datalist.jade';

export default ['igniteFormFieldInputDatalist', ['IgniteFormGUID', 'IgniteLegacyTable', (guid, LegacyTable) => {
    const link = (scope, element, attrs, [ngModel, form, label], transclude) => {
        const {id, ngModelName} = scope;

        const name = ngModelName;

        scope.id = id || guid();
        scope.form = form;
        scope.name = ngModelName + 'TextInput';
        scope.ngModel = ngModel;

        Object.defineProperty(scope, 'field', {
            get: () => scope.form[scope.name]
        });

        if (label) {
            label.for = scope.id;

            scope.label = label;

            scope.$watch('required', (required) => {
                label.required = required || false;
            });
        }

        form.$defaults = form.$defaults || {};

        if (form.$pristine) {
            if (!(_.isNull(form.$defaults[name]) || _.isUndefined(form.$defaults[name]))) {
                scope.value = form.$defaults[name];
                ngModel.$setViewValue(scope.value);
            } else
                form.$defaults[name] = _.cloneDeep(scope.value);
        }

        const setAsDefault = () => {
            if (!form.$pristine) return;

            form.$defaults = form.$defaults || {};
            form.$defaults[name] = _.cloneDeep(scope.value);
        };

        scope.$watch(() => form.$pristine, setAsDefault);
        scope.$watch('value', setAsDefault);

        const checkValid = () => {
            const input = element.find('input');

            const invalid = ngModel.$invalid || (input[0].required && !input[0].value);

            input.removeClass(invalid ? 'ng-valid' : 'ng-invalid');
            input.addClass(invalid ? 'ng-invalid' : 'ng-valid');
        };

        scope.ngChange = () => {
            ngModel.$setViewValue(scope.value);

            if (_.isEqual(scope.value, form.$defaults[name]))
                ngModel.$setPristine();
            else
                ngModel.$setDirty();

            setTimeout(checkValid, 100); // Use setTimeout() workaround of problem of two controllers.
        };

        ngModel.$render = () => {
            scope.value = ngModel.$modelValue;
        };

        scope.tableReset = () => {
            LegacyTable.tableSaveAndReset();
        };

        transclude(scope.$parent, function(clone, tscope) {
            tscope.form = form;
            tscope.ngModelName = ngModelName;

            element.find('.transclude-here').append(clone);
        });
    };

    return {
        restrict: 'E',
        scope: {
            id: '@',
            ngModelName: '@name',
            placeholder: '@',
            required: '=ngRequired',
            disabled: '=ngDisabled',
            ngBlur: '&',

            options: '=',

            focus: '=ngFocus',
            autofocus: '=igniteFormFieldInputAutofocus'
        },
        link,
        templateUrl,
        replace: true,
        transclude: true,
        require: ['ngModel', '^form', '?^igniteFormField']
    };
}]];
