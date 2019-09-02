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

import _ from 'lodash';

export default class {
    static $inject = ['$scope', 'IgniteCopyToClipboard', 'uiGridExporterService', 'uiGridExporterConstants', 'IgniteMessages', 'CSV'];

    constructor($scope, IgniteCopyToClipboard, uiGridExporterService, uiGridExporterConstants, IgniteMessages, CSV) {
        Object.assign(this, {$scope, IgniteCopyToClipboard, uiGridExporterService, uiGridExporterConstants, IgniteMessages, CSV});

        this.count = 0;
        this.visible = 0;
        this.selected = 0;
    }

    $onChanges(changes) {
        if (changes && 'gridApi' in changes && changes.gridApi.currentValue) {
            this.applyValues();

            this.gridApi.core.on.rowsVisibleChanged(this.$scope, () => {
                this.applyValues();
            });

            if (this.gridApi.selection) {
                this.gridApi.selection.on.rowSelectionChanged(this.$scope, () => this.updateSelectedCount());
                this.gridApi.selection.on.rowSelectionChangedBatch(this.$scope, () => this.updateSelectedCount());
            }
        }
    }

    updateSelectedCount() {
        if (!this.gridApi.selection)
            return;

        this.selected = this.gridApi.selection.getSelectedCount();
    }

    applyValues() {
        if (!this.gridApi.grid.rows.length) {
            this.count = 0;
            this.visible = 0;
            this.selected = 0;
            return;
        }

        this.count = this.gridApi.grid.rows.length;
        this.visible = _.sumBy(this.gridApi.grid.rows, (row) => Number(row.visible));
        this.updateSelectedCount();
    }

    copyToClipBoard() {
        if (this.count === 0 || !this.gridApi) {
            this.IgniteMessages.showError('No data to be copied');
            return;
        }

        const data = [];
        const grid = this.gridApi.grid;
        grid.options.exporterSuppressColumns = [];
        const exportColumnHeaders = this.uiGridExporterService.getColumnHeaders(grid, this.uiGridExporterConstants.VISIBLE);

        grid.rows.forEach((row) => {
            if (!row.visible)
                return;

            const values = [];

            exportColumnHeaders.forEach((exportCol) => {
                const col = grid.columns.find(({ field }) => field === exportCol.name);

                if (!col || !col.visible || col.colDef.exporterSuppressExport === true)
                    return;

                const value = grid.getCellValue(row, col);

                values.push({ value });
            });

            data.push(values);
        });

        const csvContent = this.uiGridExporterService.formatAsCsv(exportColumnHeaders, data, this.CSV.getSeparator());

        this.IgniteCopyToClipboard.copy(csvContent);
    }
}
