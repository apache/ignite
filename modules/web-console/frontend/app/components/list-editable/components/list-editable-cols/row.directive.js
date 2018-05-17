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
