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

import {nonEmpty} from 'app/utils/lodashMixins';

import {ListEditableColsController} from './cols.directive';

/** @returns {ng.IDirective} */
export default function() {
    return {
        require: '?^listEditableCols',
        /** @param {ListEditableColsController} ctrl */
        link(scope, el, attr, ctrl) {
            if (!ctrl || !ctrl.colDefs.length)
                return;

            const children = el.children();

            if (children.length !== ctrl.colDefs.length)
                return;

            if (ctrl.rowClass)
                el.addClass(ctrl.rowClass);

            ctrl.colDefs.forEach(({ cellClass }, index) => {
                if (nonEmpty(cellClass))
                    children[index].classList.add(...(Array.isArray(cellClass) ? cellClass : cellClass.split(' ')));
            });
        }
    };
}
