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

/**
 * Special decorator that fix problem in AngularStrap selectAll / deselectAll methods.
 * If this problem will be fixed in AngularStrap we can remove this delegate.
 */
angular.module('mgcrea.ngStrap.select')
    .decorator('$select', ($delegate) => {
        function SelectFactoryDecorated(element, controller, config) {
            const deligate = $delegate(element, controller, config);

            // Common vars.
            const options = angular.extend({}, $delegate.defaults, config);

            const scope = deligate.$scope;

            const selectAll = (active) => {
                var selected = [];

                scope.$apply(function () {
                    for (var i = 0; i < scope.$matches.length; i++) {
                        if (scope.$isActive(i) === active) {
                            selected[i] = scope.$matches[i].value;

                            deligate.activate(i);

                            controller.$setViewValue(scope.$activeIndex.map(function (index) {
                                if (angular.isUndefined(scope.$matches[index])) {
                                    return null;
                                }
                                return scope.$matches[index].value;
                            }));
                        }
                    }
                });

                // Emit events.
                for (var i = 0; i < selected.length; i++) {
                    if (selected[i]) {
                        scope.$emit(options.prefixEvent + '.select', selected[i], i, deligate);
                    }
                }
            };

            scope.$selectAll = () => {
                scope.$$postDigest(selectAll.bind(this, false));
            };

            scope.$selectNone = () => {
                scope.$$postDigest(selectAll.bind(this, true));
            };

            return deligate;
        }

        SelectFactoryDecorated.defaults = $delegate.defaults;

        return SelectFactoryDecorated;
    });
