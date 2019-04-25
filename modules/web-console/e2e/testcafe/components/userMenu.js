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

import {Selector, t} from 'testcafe';

const _selector = Selector('user-menu');

export const userMenu = {
    _selector,
    button: _selector.find('[bs-dropdown]'),
    /** 
     * Clicks on user menu option.
     * @param {string} label Menu option label to click on
     */
    async clickOption(label) {
        return t
            .hover(this.button)
            .click(_selector.find(`[role="menuitem"]`).withText(label));
    }
};
