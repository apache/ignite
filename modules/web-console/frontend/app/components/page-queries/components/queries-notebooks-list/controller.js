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
    static $inject = ['IgniteNotebook', 'IgniteMessages', 'IgniteLoading', 'IgniteInput', '$scope', '$modal'];

    constructor(IgniteNotebook, IgniteMessages, IgniteLoading, IgniteInput, $scope, $modal) {
        Object.assign(this, { IgniteNotebook, IgniteMessages, IgniteLoading, IgniteInput, $scope, $modal });

        this.notebooks = [];

        this.rowsToShow = 8;

        const notebookNameTemplate = `<div class="ui-grid-cell-contents notebook-name"><a ui-sref="base.sql.notebook({ noteId: row.entity._id })">{{ row.entity.name }}</a></div>`;
        const sqlQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.sqlQueriesParagraphsLength}}</div>`;
        const scanQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.scanQueriesPsaragraphsLength}}</div>`;

        const categories = [
            { name: 'Name', visible: true, enableHiding: false },
            { name: 'SQL Queries', visible: true, enableHiding: false },
            { name: 'Scan Queries', visible: true, enableHiding: false }
        ];

        const columnDefs = [
            { name: 'name', displayName: 'Notebook name', categoryDisplayName: 'Name', field: 'name', cellTemplate: notebookNameTemplate, pinnedLeft: true, filter: { placeholder: 'Filter by Name...' } },
            { name: 'sqlQueryNum', displayName: 'SQL Queries', categoryDisplayName: 'SQL Queries', field: 'sqlQueriesParagraphsLength', cellTemplate: sqlQueryTemplate, enableSorting: true, type: 'number', minWidth: 150, width: '10%', enableFiltering: false },
            { name: 'scanQueryNum', displayName: 'Scan Queries', categoryDisplayName: 'Scan Queries', field: 'scanQueriesParagraphsLength', cellTemplate: scanQueryTemplate, enableSorting: true, type: 'number', minWidth: 150, width: '10%', enableFiltering: false }
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
            enableRowSelection: true,
            enableFullRowSelection: true,
            enableColumnMenus: false,
            noUnselect: false,
            fastWatch: true,
            onRegisterApi: (api) => {
                this.gridApi = api;

                api.selection.on.rowSelectionChanged($scope, this._onSelectionChanged.bind(this));
                api.selection.on.rowSelectionChangedBatch($scope, this._onSelectionChanged.bind(this));

                this.$scope.$watch(() => this.gridApi.grid.getVisibleRows().length, (rows) => this._adjustHeight(rows));
            }
        };

        this.actionOptions = [
            {
                action: 'Clone',
                click: this.cloneNotebook.bind(this),
                available: true
            },
            {
                action: 'Rename',
                click: this.renameNotebok.bind(this),
                available: true
            },
            {
                action: 'Delete',
                click: this.deleteNotebooks.bind(this),
                available: true
            }
        ];
    }

    $onInit() {
        this._loadAllNotebooks();
    }

    async _loadAllNotebooks() {
        try {
            this.IgniteLoading.start('notebooksLoading');
            this.notebooks = await this.IgniteNotebook.read();
            this.gridOptions.data = this._preprocessNotebooksList(this.notebooks);

        } catch (err) {
            this.IgniteMessages.showError(err);

        } finally {
            this.$scope.$applyAsync();

            await this.IgniteLoading.finish('notebooksLoading');
        }
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

    _onSelectionChanged() {
        this._checkActionsAllow();
    }

    _checkActionsAllow() {
        // Dissallow clone and rename if more then one item is selectted.
        const oneItemIsSelected  = this.gridApi.selection.legacyGetSelectedRows().length === 1;
        this.actionOptions[0].available = oneItemIsSelected;
        this.actionOptions[1].available = oneItemIsSelected;
    }

    async createNotebook() {
        try {
            const newNotebookName =  await this.IgniteInput.input('New query notebook', 'Notebook name');

            this.IgniteLoading.start('notebooksLoading');
            await this.IgniteNotebook.create(newNotebookName);
            await this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();

        } catch (err) {
            this.IgniteMessages.showError(err);

        } finally {
            await this.IgniteLoading.finish('notebooksLoading');

            if (this.createNotebookModal)
                this.createNotebookModal.$promise.then(this.createNotebookModal.hide);
        }
    }

    async renameNotebok() {
        try {
            const currentNotebook =  this.gridApi.selection.legacyGetSelectedRows()[0];
            const newNotebookName =  await this.IgniteInput.input('Rename notebook', 'Notebook name', currentNotebook.name);

            if (this.getNotebooksNames().find((name) => newNotebookName === name))
                throw Error(`Notebook with name "${newNotebookName}" already exists!`);

            this.IgniteLoading.start('notebooksLoading');
            await this.IgniteNotebook.save(Object.assign(currentNotebook, {name: newNotebookName}));

        } catch (err) {
            this.IgniteMessages.showError(err);

        } finally {
            await this.IgniteLoading.finish('notebooksLoading');
            this._loadAllNotebooks();
        }
    }

    async cloneNotebook() {
        try {
            const clonedNotebook = Object.assign({}, this.gridApi.selection.legacyGetSelectedRows()[0]);
            const newNotebookName = await this.IgniteInput.clone(clonedNotebook.name, this.getNotebooksNames());

            this.IgniteLoading.start('notebooksLoading');
            await this.IgniteNotebook.clone(newNotebookName, clonedNotebook);
            await this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();

        } catch (err) {
            this.IgniteMessages.showError(err);

        } finally {
            await this.IgniteLoading.finish('notebooksLoading');

            if (this.createNotebookModal)
                this.createNotebookModal.$promise.then(this.createNotebookModal.hide);
        }
    }

    getNotebooksNames() {
        return this.notebooks.map((notebook) => notebook.name);
    }

    async deleteNotebooks() {
        try {
            this.IgniteLoading.start('notebooksLoading');
            await this.IgniteNotebook.removeBatch(this.gridApi.selection.legacyGetSelectedRows());
            await this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();

        } catch (err) {
            this.IgniteMessages.showError(err);

            await this.IgniteLoading.finish('notebooksLoading');
        }
    }

    _adjustHeight(rows) {
        // Add header height.
        const height = Math.min(rows, this.rowsToShow) * 48 + 78;

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }
}
