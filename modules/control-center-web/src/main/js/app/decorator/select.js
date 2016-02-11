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

import angular from 'angular';

angular.module('mgcrea.ngStrap.select')
    .decorator('$select', ($delegate) => {
        function SelectFactoryDecorated(element, controller, config) {
            const deligate = $delegate(element, controller, config);

            const scope = deligate.$scope;

            const $selectAllDeligate = scope.$selectAll;

            scope.$selectAll = () => {
                if (scope.$isMultiple) {
                    const newActiveIndex = [];

                    for (let i = 0; i < scope.$matches.length; i++)
                        newActiveIndex.push(i);

                    scope.$activeIndex = newActiveIndex;

                    controller.$setViewValue(scope.$activeIndex.map((index) => {
                        if (angular.isUndefined(scope.$matches[index]))
                            return null;

                        return scope.$matches[index].value;
                    }));
                }
                else
                    $selectAllDeligate();
            };

            const $selectNoneDeligate = scope.$selectNone;

            scope.$selectNone = () => {
                if (scope.$isMultiple) {
                    scope.$activeIndex = [];

                    controller.$setViewValue([]);
                }
                else
                    $selectNoneDeligate();
            };

            return deligate;
        }

        SelectFactoryDecorated.defaults = $delegate.defaults;

        return SelectFactoryDecorated;
    });
