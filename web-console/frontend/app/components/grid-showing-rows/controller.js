

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

        this.visible = _.sumBy(this.gridApi.grid.rows, (row) => {
            if (!row.visible)
                return 0;

            const treeNode = row.treeNode;

            if (!treeNode)
                return 1;

            let parent = treeNode.parentRow;

            while (parent) {
                if (parent.treeNode.state !== 'expanded')
                    return 0;

                parent = parent.treeNode.parentRow;
            }

            return 1;
        });

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
