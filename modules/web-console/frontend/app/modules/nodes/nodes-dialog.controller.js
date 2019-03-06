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

const NID_TEMPLATE = '<div class="ui-grid-cell-contents" title="{{ COL_FIELD }}">{{ COL_FIELD | limitTo:8 }}</div>';

const COLUMNS_DEFS = [
    {displayName: 'Node ID8', field: 'nid', headerTooltip: 'Node ID8', cellTemplate: NID_TEMPLATE, minWidth: 85, width: 145, pinnedLeft: true},
    {displayName: 'Node IP', field: 'ip', headerTooltip: 'Primary IP address of node', minWidth: 100, width: 150},
    {displayName: 'Grid name', field: 'gridName', headerTooltip: 'Name of node grid cluster', minWidth: 110, width: 150},
    {displayName: 'Version', field: 'version', headerTooltip: 'Node version', minWidth: 75, width: 140},
    {displayName: 'OS information', field: 'os', headerTooltip: 'OS information for node\'s host', minWidth: 125}
];

export default function controller($scope, $animate, uiGridConstants, nodes, options) {
    const $ctrl = this;

    const updateSelected = () => {
        const nids = $ctrl.gridApi.selection.legacyGetSelectedRows().map((node) => node.nid).sort();

        if (!_.isEqual(nids, $ctrl.selected))
            $ctrl.selected = nids;
    };

    $ctrl.nodes = nodes;
    $ctrl.options = options;
    $ctrl.selected = [];

    $ctrl.gridOptions = {
        data: nodes,
        columnVirtualizationThreshold: 30,
        columnDefs: COLUMNS_DEFS,
        enableRowSelection: true,
        enableRowHeaderSelection: false,
        enableColumnMenus: false,
        multiSelect: true,
        modifierKeysToMultiSelect: true,
        noUnselect: false,
        flatEntityAccess: true,
        fastWatch: true,
        onRegisterApi: (api) => {
            $animate.enabled(api.grid.element, false);

            $ctrl.gridApi = api;

            api.selection.on.rowSelectionChanged($scope, updateSelected);
            api.selection.on.rowSelectionChangedBatch($scope, updateSelected);

            $ctrl.gridApi.grid.element.css('height', '270px');

            setTimeout(() => $ctrl.gridApi.core.notifyDataChange(uiGridConstants.dataChange.COLUMN), 300);
        },
        ...options.grid
    };
}

controller.$inject = ['$scope', '$animate', 'uiGridConstants', 'nodes', 'options'];
