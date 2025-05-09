

import _ from 'lodash';

import {columnDefsFn} from './column-defs';

export default class ConnectedClustersListCtrl {
    data: any;
    gridOptions: uiGrid.IGridOptionsOf<this['data']>;
    gridApi: uiGrid.IGridApiOf<this['data']>;

    static $inject = ['$scope', 'AgentManager', '$translate'];

    constructor(
        private $scope: ng.IScope,
        private agentMgr,
        private $translate: ng.translate.ITranslateService
    ) {}

    $onInit() {
        this.gridOptions = {
            data: _.orderBy(this.data, ['name'], ['asc']),
            columnDefs: columnDefsFn(this.$translate),
            columnVirtualizationThreshold: 30,
            rowHeight: 46,
            enableColumnMenus: false,
            enableFullRowSelection: true,
            enableFiltering: false,
            selectionRowHeaderWidth: 52,
            onRegisterApi: (api) => {
                this.gridApi = api;

                this.$scope.$watch(() => this.gridApi.grid.getVisibleRows().length, (rows) => this.adjustHeight(rows));
            }
        };
    }

    adjustHeight(rows) {
        // One row for grid-no-data.
        const height = Math.max(1, Math.min(rows, 6)) * 48 + 49 + (rows ? 8 : 0);

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }
}
