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

import template from './template.pug';
import './style.scss';
import {IUiGridConstants} from 'ui-grid';

export default function pcUiGridFilters(uiGridConstants: IUiGridConstants) {
    return {
        require: 'uiGrid',
        link: {
            pre(scope, el, attr, grid) {
                if (!grid.grid.options.enableFiltering)
                    return;

                grid.grid.options.columnDefs.filter((cd) => cd.multiselectFilterOptions).forEach((cd) => {
                    cd.headerCellTemplate = template;
                    cd.filter = {
                        type: uiGridConstants.filter.SELECT,
                        term: cd.multiselectFilterOptions.map((t) => t.value),
                        condition(searchTerm, cellValue, row, column) {
                            return searchTerm.includes(cellValue);
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
                });
            }
        }
    } as ng.IDirective;
}

pcUiGridFilters.$inject = ['uiGridConstants'];
