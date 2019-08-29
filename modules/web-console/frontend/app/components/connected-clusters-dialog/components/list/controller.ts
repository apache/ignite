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
