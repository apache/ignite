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

import headerTemplate from '../../../../../app/primitives/ui-grid-header/index.tpl.pug';

export class NotebooksListCtrl {
    static $inject = ['IgniteNotebook', '$scope'];

    constructor(IgniteNotebook, $scope) {
        Object.assign(this, { IgniteNotebook, $scope });

        const notebookNameTemplate = `<div class="ui-grid-cell-contents"><a ui-sref="base.sql.tabs.notebook({ noteId: row.entity._id })">{{ row.entity.name }}</a></div>`;

        const categories = [
            { name: 'Name', visible: true, enableHiding: false },
            { name: 'SQL Query', visible: true, enableHiding: false, enableFiltering: false },
            { name: 'Scan Query', visible: true, enableHiding: false, enableFiltering: false }
        ];

        const columnDefs = [
            { name: 'name', displayName: 'Name', categoryDisplayName: 'Name', field: 'name', cellTemplate: notebookNameTemplate, minWidth: 150, width: 550, filter: { placeholder: 'Filter by Name...' } },
            { name: 'sqlQueryNum', displayName: 'SQL Query', categoryDisplayName: 'SQL Query', field: 'sqlQueryNum', minWidth: 150, width: 150 },
            { name: 'scanQueryNum', displayName: 'Scan Query', categoryDisplayName: 'Scan Query', field: 'scanQueryNum', minWidth: 150, width: 150 }
        ];

        this.gridOptions = {
            data: [],

            categories,
            columnDefs,
            headerTemplate,

            rowHeight: 46,
            selectWithCheckboxOnly: true,
            suppressRemoveSort: false,
            enableFiltering: true,
            enableSelectAll: true,
            enableRowSelection: false,
            enableFullRowSelection: true,
            enableColumnMenus: false,
            noUnselect: false,
            fastWatch: true,
            onRegisterApi: (api) => {
                this.gridApi = api;

                this.$scope.$watch(() => this.gridApi.grid.getVisibleRows().length, (rows) => this._adjustHeight(rows));
            }
        };
    }

    async $onInit() {
        this.notebooks = await this._loadAllNotebooks();
        this.gridOptions.data = this.notebooks;
    }

    _loadAllNotebooks() {
        return this.IgniteNotebook.read();
    }

    _adjustHeight(rows) {
        // Add header height.
        const height = Math.min(rows, 11) * 48 + 78;

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }
}
