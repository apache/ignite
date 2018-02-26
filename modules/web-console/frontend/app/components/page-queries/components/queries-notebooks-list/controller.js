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

        const notebookNameTemplate = `<div class="ui-grid-cell-contents notebook-name"><a ui-sref="base.sql.tabs.notebook({ noteId: row.entity._id })">{{ row.entity.name }}</a></div>`;
        const sqlQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.sqlQueriesParagraphsLength}}</div>`;
        const scanQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.scanQueriesPsaragraphsLength}}</div>`;

        const categories = [
            { name: 'Name', visible: true, enableHiding: false },
            { name: 'SQL Query', visible: true, enableHiding: false },
            { name: 'Scan Query', visible: true, enableHiding: false }
        ];

        const columnDefs = [
            { name: 'name', displayName: 'Name', categoryDisplayName: 'Name', field: 'name', cellTemplate: notebookNameTemplate, pinnedLeft: true, filter: { placeholder: 'Filter by Name...' } },
            { name: 'sqlQueryNum', displayName: 'SQL Query', categoryDisplayName: 'SQL Query', field: 'sqlQueryNum', cellTemplate: sqlQueryTemplate, minWidth: 150, width: 150, enableFiltering: false },
            { name: 'scanQueryNum', displayName: 'Scan Query', categoryDisplayName: 'Scan Query', field: 'scanQueryNum', cellTemplate: scanQueryTemplate, minWidth: 150, width: 150, enableFiltering: false }
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
        this.gridOptions.data = this._preprocessNotebooksList(this.notebooks);
    }

    _preprocessNotebooksList(notebooks = []) {
        return notebooks.map((notebook) => {
            notebook.sqlQueriesParagraphsLength = this._countParagraphs(notebook, 'query');
            notebook.scanQueriesPsaragraphsLength = this._countParagraphs(notebook, 'scan');

            return notebook;
        });
    }

    _countParagraphs(notebook, queryType = 'query') {
        return notebook.paragraphs.filter((paragraph) => paragraph.qryType === queryType).length || 0;
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
