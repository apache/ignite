/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import _ from 'lodash';

import columnDefs from './column-defs';

export default class ConnectedClustersListCtrl {
    static $inject = ['$scope', 'AgentManager'];

    /**
     * @param {ng.IScope} $scope
     * @param {import('app/modules/agent/AgentManager.service').default} agentMgr
     */
    constructor($scope, agentMgr) {
        this.$scope = $scope;
        this.agentMgr = agentMgr;
    }

    $onInit() {
        this.gridOptions = {
            data: _.orderBy(this.data, ['name'], ['asc']),
            columnDefs,
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
