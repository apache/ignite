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

export default ['igniteFormFieldLabel', [() => {
    return {
        restrict: 'E',
        compile() {
            return {
                post($scope, $element, $attrs, [form, field], $transclude) {
                    $transclude($scope, function(clone) {
                        const text = clone.text();

                        if (/(.*):$/.test(text))
                            field.name = /(.*):$/.exec(text)[1];

                        const $label = $element.parent().parent().find('.group-legend > label, .ignite-field > label');

                        if ($label[0] && $element[0].id) {
                            const id = $element[0].id;

                            $label[0].id = id.indexOf('+') >= 0 ? $scope.$eval(id) : id;
                        }

                        $label.append(clone);
                    });
                }
            };
        },
        replace: true,
        transclude: true,
        require: ['^form', '?^igniteFormField']
    };
}]];
