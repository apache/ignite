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

import template from './group.jade!';

export default ['igniteFormGroup', [() => {
    const controller = [function() { }];

    const link = (scope, el, attrs, [ngModelCtrl, ownFormCtrl, parentFormCtrl]) => {
        if (!ownFormCtrl)
            return;

        const name = attrs.ngForm;
        ngModelCtrl.$name = name;

        parentFormCtrl.$addControl(ngModelCtrl);
        parentFormCtrl.$removeControl(ownFormCtrl);

        scope.value = scope.value || [];
        parentFormCtrl.$defaults = parentFormCtrl.$defaults || {};
        parentFormCtrl.$defaults[name] = _.cloneDeep(scope.value);

        const setAsDefault = () => {
            if (!parentFormCtrl.$pristine)
                return;

            scope.value = scope.value || [];
            parentFormCtrl.$defaults = parentFormCtrl.$defaults || {};
            parentFormCtrl.$defaults[name] = _.cloneDeep(scope.value);
        };

        const setAsDirty = () => {
            if (JSON.stringify(scope.value) !== JSON.stringify(parentFormCtrl.$defaults[name]))
                ngModelCtrl.$setDirty();
            else
                ngModelCtrl.$setPristine();
        };

        scope.$watch(() => parentFormCtrl.$pristine, setAsDefault);

        scope.$watch('value', setAsDefault);
        scope.$watch('value', setAsDirty, true);
    };

    return {
        restrict: 'E',
        scope: {
            value: '=ngModel'
        },
        bindToController: {
            label: '@'
        },
        link,
        template,
        controller,
        controllerAs: 'group',
        replace: true,
        transclude: true,
        require: ['?ngModel', '?form', '^^form']
    };
}]];
