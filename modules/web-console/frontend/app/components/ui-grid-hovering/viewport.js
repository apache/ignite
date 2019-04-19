/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import angular from 'angular';

export default function() {
    return {
        priority: -200,
        compile($el) {
            let newNgClass = '';

            const rowRepeatDiv = angular.element($el.children().children()[0]);
            const existingNgClass = rowRepeatDiv.attr('ng-class');

            if (existingNgClass)
                newNgClass = existingNgClass.slice(0, -1) + ', "ui-grid-row-hovered": row.isHovered }';
            else
                newNgClass = '{ "ui-grid-row-hovered": row.isHovered }';

            rowRepeatDiv.attr('ng-class', newNgClass);

            return {
                pre() { },
                post() { }
            };
        }
    };
}
