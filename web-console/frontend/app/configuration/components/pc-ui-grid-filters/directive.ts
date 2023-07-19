

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
