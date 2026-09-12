

export default class {
    static $inject = ['$scope', 'uiGridConstants'];

    constructor($scope, uiGridConstants) {
        Object.assign(this, {$scope, uiGridConstants});
    }

    $onChanges(changes) {
        if (changes && 'gridApi' in changes && changes.gridApi.currentValue && this.gridApi.selection) {
            this.applyValues();

            this.gridApi.grid.registerDataChangeCallback(() => this.applyValues(), [this.uiGridConstants.dataChange.ROW]);
            // Used to toggle selected of one row.
            this.gridApi.selection.on.rowSelectionChanged(this.$scope, () => this.applyValues());
            // Used to toggle all selected of rows.
            this.gridApi.selection.on.rowSelectionChangedBatch(this.$scope, () => this.applyValues());
        }
    }

    applyValues() {
        this.selected = this.gridApi.selection.legacyGetSelectedRows().length;
        this.count = this.gridApi.grid.rows.length;
    }
}
