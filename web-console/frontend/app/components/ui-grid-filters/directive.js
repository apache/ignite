

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
