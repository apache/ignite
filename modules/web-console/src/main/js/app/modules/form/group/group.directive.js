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

import templateUrl from './group.jade';

export default ['igniteFormGroup', [() => {
    const controller = [function() { }];

    const link = (scope, el, attrs, [ngModelCtrl, ownFormCtrl, parentFormCtrl]) => {
        if (!ownFormCtrl)
            return;

        const name = attrs.ngForm;
        ngModelCtrl.$name = name;

        parentFormCtrl.$addControl(ngModelCtrl);
        parentFormCtrl.$removeControl(ownFormCtrl);

        scope.ngModel = scope.ngModel || [];
        parentFormCtrl.$defaults = parentFormCtrl.$defaults || {};

        if (parentFormCtrl.$pristine) {
            if (!(_.isNull(parentFormCtrl.$defaults[name]) || _.isUndefined(parentFormCtrl.$defaults[name])))
                scope.ngModel = parentFormCtrl.$defaults[name];
            else
                parentFormCtrl.$defaults[name] = _.cloneDeep(scope.ngModel);
        }

        const setAsDefault = () => {
            if (!parentFormCtrl.$pristine)
                return;

            scope.ngModel = scope.ngModel || [];
            parentFormCtrl.$defaults = parentFormCtrl.$defaults || {};
            parentFormCtrl.$defaults[name] = _.cloneDeep(scope.ngModel);
        };

        const setAsDirty = () => {
            if (_.isEqual(scope.ngModel, parentFormCtrl.$defaults[name]))
                ngModelCtrl.$setPristine();
            else
                ngModelCtrl.$setDirty();
        };

        scope.$watch(() => parentFormCtrl.$pristine, setAsDefault);

        scope.$watch('ngModel', setAsDefault);
        scope.$watch('ngModel', setAsDirty, true);
    };

    return {
        restrict: 'E',
        scope: {
            ngModel: '=ngModel'
        },
        bindToController: {
            label: '@'
        },
        link,
        templateUrl,
        controller,
        controllerAs: 'group',
        replace: true,
        transclude: true,
        require: ['?ngModel', '?form', '^^form']
    };
}]];
