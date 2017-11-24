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

const template = '<i class="tipField icon-help"></i>';

export default ['igniteFormFieldTooltip', ['$tooltip', ($tooltip) => {
    const link = ($scope, $element, $attrs, [form, field], $transclude) => {
        const content = Array.prototype.slice
            .apply($transclude($scope))
            .reduce((html, el) => html += el.outerHTML || el.textContent || el, '');

        $tooltip($element, { title: content });

        if (field)
            $element.attr('id', field.for + 'Tooltip');

        // TODO cleanup css styles.
        if ($element.hasClass('tipLabel'))
            $element.removeClass('tipField');

        if ($element.parent('label').length)
            $element.addClass('tipLabel').removeClass('tipField');
    };

    return {
        priority: 1,
        restrict: 'E',
        scope: {},
        template,
        link,
        replace: true,
        transclude: true,
        require: ['^form', '?^igniteFormField']
    };
}]];
