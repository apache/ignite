/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import angular from 'angular';
import _ from 'lodash';

/**
 * Special decorator that fix problem in AngularStrap selectAll / deselectAll methods.
 * If this problem will be fixed in AngularStrap we can remove this delegate.
 */
export default angular.module('mgcrea.ngStrap.select')
    .decorator('$select', ['$delegate', function($delegate) {
        function SelectFactoryDecorated(element, controller, config) {
            const delegate = $delegate(element, controller, config);

            // Common vars.
            const options = Object.assign({}, $delegate.defaults, config);

            const scope = delegate.$scope;

            const valueByIndex = (index) => {
                if (_.isUndefined(scope.$matches[index]))
                    return null;

                return scope.$matches[index].value;
            };

            const selectAll = (active) => {
                const selected = [];

                scope.$apply(() => {
                    for (let i = 0; i < scope.$matches.length; i++) {
                        if (scope.$isActive(i) === active) {
                            selected[i] = scope.$matches[i].value;

                            delegate.activate(i);

                            controller.$setViewValue(scope.$activeIndex.map(valueByIndex));
                        }
                    }
                });

                // Emit events.
                for (let i = 0; i < selected.length; i++) {
                    if (selected[i])
                        scope.$emit(options.prefixEvent + '.select', selected[i], i, delegate);
                }
            };

            scope.$selectAll = () => {
                scope.$$postDigest(selectAll.bind(this, false));
            };

            scope.$selectNone = () => {
                scope.$$postDigest(selectAll.bind(this, true));
            };

            return delegate;
        }

        SelectFactoryDecorated.defaults = $delegate.defaults;

        return SelectFactoryDecorated;
    }]);
