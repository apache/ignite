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

const template = '<i class="tipField fa fa-arrow-up ng-scope" ng-click="up()"></i>';

export default ['igniteFormFieldUp', ['$tooltip', ($tooltip) => {
    const link = (scope, $element) => {
        $tooltip($element, { title: 'Move item up' });

        scope.up = () => {
            const idx = scope.models.indexOf(scope.model);

            scope.models.splice(idx, 1);
            scope.models.splice(idx - 1, 0, scope.model);
        };
    };

    return {
        restrict: 'E',
        scope: {
            model: '=ngModel',
            models: '=models'
        },
        template,
        link,
        replace: true,
        transclude: true,
        require: '^form'
    };
}]];
