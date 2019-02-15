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

import template from './template.pug';
import './style.scss';

export default function uiGridFilters(uiGridConstants) {
    return {
        require: 'uiGrid',
        link: {
            pre(scope, el, attr, gridApi) {
                if (!gridApi.grid.options.enableFiltering)
                    return;

                const applyMultiselectFilter = (cd) => {
                    if (!cd.headerCellTemplate)
                        cd.headerCellTemplate = template;

                    cd.filter = {
                        type: uiGridConstants.filter.SELECT,
                        term: cd.multiselectFilterOptions.map((t) => t.value),
                        condition(searchTerm, cellValue) {
                            if (cellValue)
                                return Array.isArray(cellValue) ? _.intersection(searchTerm, cellValue).length : searchTerm.includes(cellValue);

                            return true;
                        },
                        selectOptions: cd.multiselectFilterOptions,
                        $$selectOptionsMapping: cd.multiselectFilterOptions.reduce((a, v) => Object.assign(a, {[v.value]: v.label}), {}),
                        $$multiselectFilterTooltip() {
                            const prefix = 'Active filter';
                            switch (this.term.length) {
                                case 0:
                                    return `${prefix}: show none`;
                                default:
                                    return `${prefix}: ${this.term.map((t) => this.$$selectOptionsMapping[t]).join(', ')}`;
                                case this.selectOptions.length:
                                    return `${prefix}: show all`;
                            }
                        }
                    };
                    if (!cd.cellTemplate) {
                        cd.cellTemplate = `
                            <div class="ui-grid-cell-contents">
                                {{ col.colDef.filter.$$selectOptionsMapping[row.entity[col.field]] }}
                            </div>
                        `;
                    }
                };

                const updateMultiselectOptionsHandler = (gridApi, colDef) => {
                    if (!gridApi)
                        return;

                    const col = gridApi.grid.getColumn(colDef.name);
                    const selectOptions = colDef.multiselectFilterOptionsFn(gridApi.grid, col.filter);

                    if (selectOptions.length === col.filter.selectOptions.length)
                        return;

                    col.filter.term = selectOptions.map((t) => t.value);
                    col.filter.selectOptions = selectOptions;
                };

                gridApi.grid.options.columnDefs.filter((cd) => cd.multiselectFilterOptions).forEach(applyMultiselectFilter);

                gridApi.grid.options.columnDefs.filter((cd) => cd.multiselectFilterOptionsFn).forEach((cd) => {
                    cd.multiselectFilterOptions = cd.multiselectFilterOptions || [];
                    applyMultiselectFilter(cd);

                    if (cd.multiselectFilterDialog)
                        cd.filter.selectDialog = cd.multiselectFilterDialog;

                    gridApi.grid.api.core.on.rowsVisibleChanged(scope, (gridApi) => updateMultiselectOptionsHandler(gridApi, cd));
                });
            }
        }
    };
}

uiGridFilters.$inject = ['uiGridConstants'];
