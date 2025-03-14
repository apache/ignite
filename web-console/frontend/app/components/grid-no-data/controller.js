

import _ from 'lodash';

export default class {
    static $inject = ['$scope', 'uiGridConstants'];

    constructor($scope, uiGridConstants) {
        Object.assign(this, {$scope, uiGridConstants});

        this.noData = true;
    }

    $onChanges(changes) {
        if (changes && 'gridApi' in changes && changes.gridApi.currentValue) {
            this.applyValues();

            this.gridApi.core.on.rowsVisibleChanged(this.$scope, () => {
                this.applyValues();
            });
        }
    }

    applyValues() {
        if (!this.gridApi.grid.rows.length) {
            this.noData = true;

            return;
        }

        this.noData = false;

        this.noDataFiltered = _.sumBy(this.gridApi.grid.rows, 'visible') === 0;
    }
}
