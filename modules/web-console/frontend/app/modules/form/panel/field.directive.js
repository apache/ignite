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

export default ['igniteFormPanelField', ['$parse', 'IgniteLegacyTable', ($parse, LegacyTable) => {
    const link = (scope, element, attrs, [ngModelCtrl, formCtrl]) => {
        formCtrl.$defaults = formCtrl.$defaults || {};

        const { name, ngModel } = attrs;
        const getter = () => $parse(ngModel)(scope);

        const saveDefault = () => {
            formCtrl.$defaults[name] = _.cloneDeep(getter());
        };

        const resetDefault = () => {
            ngModelCtrl.$viewValue = formCtrl.$defaults[name];

            ngModelCtrl.$valid = true;
            ngModelCtrl.$invalid = false;
            ngModelCtrl.$error = {};
            ngModelCtrl.$render();
        };

        if (!(_.isNull(formCtrl.$defaults[name]) || _.isUndefined(formCtrl.$defaults[name])))
            resetDefault();
        else
            saveDefault();

        scope.tableReset = (trySave) => {
            if (trySave === false || !LegacyTable.tableSaveAndReset())
                LegacyTable.tableReset();
        };

        scope.$watch(() => formCtrl.$pristine, () => {
            if (!formCtrl.$pristine)
                return;

            saveDefault();
            resetDefault();
        });

        scope.$watch(() => ngModelCtrl.$modelValue, () => {
            if (!formCtrl.$pristine)
                return;

            saveDefault();
        });
    };

    return {
        restrict: 'A',
        link,
        require: ['ngModel', '^form']
    };
}]];
