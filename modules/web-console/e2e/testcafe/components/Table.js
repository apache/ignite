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

import {Selector, t} from 'testcafe';

const findCell = Selector((table, rowIndex, columnLabel) => {
    table = table();

    const columnIndex = [].constructor.from(
        table.querySelectorAll('.ui-grid-header-cell:not(.ui-grid-header-span)'),
        (e) => e.textContent
    ).findIndex((t) => t.includes(columnLabel));

    const row = table.querySelector(`.ui-grid-render-container:not(.left) .ui-grid-viewport .ui-grid-row:nth-of-type(${rowIndex + 1})`);
    const cell = row.querySelector(`.ui-grid-cell:nth-of-type(${columnIndex})`);

    return cell;
});

export class Table {
    /** @param {ReturnType<Selector>} selector */
    constructor(selector) {
        this._selector = selector;
        this.title = this._selector.find('.panel-title');
        this.actionsButton = this._selector.find('.btn-ignite').withText('Actions');
        this.allItemsCheckbox = this._selector.find('[role="checkbox button"]');
    }

    /** @param {string} label */
    async performAction(label) {
        await t.hover(this.actionsButton).click(Selector('.dropdown-menu a').withText(label));
    }

    /**
     * Toggles grid row selection
     * @param {number} index Index of row, starting with 1
     */
    async toggleRowSelection(index) {
        await t.click(this._selector.find(`.ui-grid-pinned-container .ui-grid-row:nth-of-type(${index}) .ui-grid-selection-row-header-buttons`));
    }

    /**
     * @param {number} rowIndex
     * @param {string} columnLabel
     */
    findCell(rowIndex, columnLabel) {
        return Selector(findCell(this._selector, rowIndex, columnLabel));
    }
}
