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

import debounce from 'lodash/debounce';
import headerTemplate from 'app/primitives/ui-grid-header/index.tpl.pug';

export default class IgniteUiGrid {
    /** @type */
    gridApi;

    /** @type */
    items;

    /** @type */
    columnDefs;

    /** @type */
    categories;

    /** @type */
    onSelectionChange;

    static $inject = ['$scope', '$element', 'gridUtil'];

    /**
     * @param {ng.IScope} $scope
     */
    constructor($scope, $element, gridUtil) {
        this.$scope = $scope;
        this.$element = $element;
        this.gridUtil = gridUtil;
        this.selectedRows = [];

        this.rowIdentityKey = '_id';
    }

    $onInit() {
        this.SCROLLBAR_WIDTH = this.gridUtil.getScrollbarWidth();

        this.grid = {
            data: this.items,
            columnDefs: this.columnDefs,
            categories: this.categories,
            columnVirtualizationThreshold: 30,
            rowHeight: 48,
            headerRowHeight: 70,
            enableColumnMenus: false,
            enableFullRowSelection: true,
            enableFiltering: true,
            enableRowHashing: false,
            fastWatch: true,
            showTreeExpandNoChildren: false,
            multiSelect: true,
            selectionRowHeaderWidth: 30,
            exporterCsvFilename: `${_.camelCase([this.tabName, this.tableTitle])}.csv`,
            onRegisterApi: (api) => {
                this.gridApi = api;

                api.core.on.rowsVisibleChanged(this.$scope, () => {
                    this.adjustHeight();
                });

                if (this.onSelectionChange) {
                    api.selection.on.rowSelectionChanged(this.$scope, (row, e) => {
                        this.onRowsSelectionChange([row], e);
                    });

                    api.selection.on.rowSelectionChangedBatch(this.$scope, (rows, e) => {
                        this.onRowsSelectionChange(rows, e);
                    });
                }
            }
        };

        if (this.grid.categories)
            this.grid.headerTemplate = headerTemplate;
    }

    $onChanges(changes) {
        const hasChanged = (binding) =>
            binding in changes && changes[binding].currentValue !== changes[binding].previousValue;

        if (hasChanged('items') && this.grid) {
            this.grid.data = changes.items.currentValue;
            this.gridApi.grid.modifyRows(this.grid.data);

            this.adjustHeight();

            if (this.onSelectionChange)
                this.applyIncomingSelection(this.selectedRows);
        }
    }

    applyIncomingSelection(selected = []) {
        this.gridApi.selection.clearSelectedRows({ ignore: true });

        const rows = this.grid.data.filter((r) =>
            selected.map((row) =>
                row[this.rowIdentityKey]).includes(r[this.rowIdentityKey]));

        rows.forEach((r) => {
            this.gridApi.selection.selectRow(r, { ignore: true });
        });
    }

    onRowsSelectionChange = debounce((rows, e = {}) => {
        if (e.ignore) return;
        this.selectedRows = this.gridApi.selection.legacyGetSelectedRows();

        if (this.onSelectionChange)
            this.onSelectionChange({ $event: this.selectedRows });
    });

    adjustHeight() {
        const maxRowsToShow = this.maxRowsToShow || 5;
        const headerBorder = 1;
        const visibleRows = this.gridApi.core.getVisibleRows().length;
        const header = this.grid.headerRowHeight + headerBorder;
        const optionalScroll = (visibleRows ? this.gridUtil.getScrollbarWidth() : 0);
        const height = Math.min(visibleRows, maxRowsToShow) * this.grid.rowHeight + header + optionalScroll;

        this.gridApi.grid.element.css('height', height + 'px');
        this.gridApi.core.handleWindowResize();
    }
}
