

import {DemoService} from 'app/modules/demo/Demo.module';
import _ from 'lodash';

export class NotebooksListCtrl {
    static $inject = ['IgniteNotebook', 'IgniteMessages', 'IgniteLoading', 'IgniteInput', '$scope', '$modal', 'Demo', '$translate'];

    constructor(
        IgniteNotebook,
        IgniteMessages,
        IgniteLoading,
        IgniteInput,
        $scope,
        $modal,
        private Demo: DemoService,
        private $translate: ng.translate.ITranslateService
    ) {
        Object.assign(this, { IgniteNotebook, IgniteMessages, IgniteLoading, IgniteInput, $scope, $modal });

        this.notebooks = [];

        this.rowsToShow = 8;

        const notebookNameTemplate = `<div class="ui-grid-cell-contents notebook-name"><a ui-sref="base.sql.notebook({ noteId: row.entity.id })">{{ row.entity.name }}</a></div>`;
        const sqlQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.sqlQueriesParagraphsLength}}</div>`;
        const scanQueryTemplate = `<div class="ui-grid-cell-contents">{{row.entity.scanQueriesPsaragraphsLength}}</div>`;

        this.columnDefs = [
            {
                name: 'name',
                displayName: this.$translate.instant('queries.notebooks.gridColumns.name.displayName'),
                categoryDisplayName: this.$translate.instant('queries.notebooks.gridColumns.name.categoryDisplayName'),
                field: 'name',
                cellTemplate: notebookNameTemplate,
                filter: { placeholder: this.$translate.instant('queries.notebooks.gridColumns.name.filterPlaceholder') }
            },
            {
                name: 'sqlQueryNum',
                displayName: this.$translate.instant('queries.notebooks.gridColumns.sqlQueries.displayName'),
                categoryDisplayName: this.$translate.instant('queries.notebooks.gridColumns.sqlQueries.categoryDisplayName'),
                field: 'sqlQueriesParagraphsLength',
                cellTemplate: sqlQueryTemplate,
                enableSorting: true, type: 'number',
                minWidth: 150,
                width: '10%',
                enableFiltering: false
            },
            {
                name: 'scanQueryNum',
                displayName: this.$translate.instant('queries.notebooks.gridColumns.scanQueries.displayName'),
                categoryDisplayName: this.$translate.instant('queries.notebooks.gridColumns.scanQueries.categoryDisplayName'),
                field: 'scanQueriesParagraphsLength',
                cellTemplate: scanQueryTemplate,
                enableSorting: true,
                type: 'number',
                minWidth: 150,
                width: '10%',
                enableFiltering: false
            }
        ];

        this.actionOptions = [
            {
                action: this.$translate.instant('queries.notebooks.actions.clone'),
                click: this.cloneNotebook.bind(this),
                available: true
            },
            {
                action: this.$translate.instant('queries.notebooks.actions.rename'),
                click: this.renameNotebook.bind(this),
                available: true
            },
            {
                action: this.$translate.instant('queries.notebooks.actions.delete'),
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

            const data = await this.IgniteNotebook.read();

            this.notebooks = this._preprocessNotebooksList(data);
        }
        catch (err) {
            this.IgniteMessages.showError(err);
        }
        finally {
            this.$scope.$applyAsync();

            this.IgniteLoading.finish('notebooksLoading');
        }
    }

    _preprocessNotebooksList(notebooks = []) {
        return notebooks.map((notebook) => {
            notebook.sqlQueriesParagraphsLength = this._countParagraphs(notebook, 'SQL_FIELDS');
            notebook.scanQueriesPsaragraphsLength = this._countParagraphs(notebook, 'SCAN');

            return notebook;
        });
    }

    _countParagraphs(notebook, queryType = 'SQL_FIELDS') {
        return _.filter(notebook.paragraphs, (paragraph) => paragraph.queryType === queryType).length;
    }

    onSelectionChanged() {
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
            const newNotebookName = await this.IgniteInput.input(
                this.$translate.instant('queries.createNotebookDialog.title'),
                this.$translate.instant('queries.createNotebookDialog.nameInput.title')
            );

            this.IgniteLoading.start('notebooksLoading');

            await this.IgniteNotebook.create(newNotebookName);

            this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();
        }
        catch (err) {
            this.IgniteMessages.showError(err);
        }
        finally {
            this.IgniteLoading.finish('notebooksLoading');

            if (this.createNotebookModal)
                this.createNotebookModal.$promise.then(this.createNotebookModal.hide);
        }
    }

    async renameNotebook() {
        try {
            const currentNotebook = this.gridApi.selection.legacyGetSelectedRows()[0];
            const newNotebookName = await this.IgniteInput.input(
                this.$translate.instant('queries.renameNotebookDialog.title'),
                this.$translate.instant('queries.renameNotebookDialog.nameInput.title'),
                currentNotebook.name
            );

            if (this.getNotebooksNames().find((name) => newNotebookName === name))
                throw Error(this.$translate.instant('queries.notebookNameCollissionErrorMessage', {name: newNotebookName}));

            this.IgniteLoading.start('notebooksLoading');

            await this.IgniteNotebook.save(Object.assign(currentNotebook, {name: newNotebookName}));
        }
        catch (err) {
            this.IgniteMessages.showError(err);
        }
        finally {
            this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();
        }
    }

    async cloneNotebook() {
        try {
            const clonedNotebook = Object.assign({}, this.gridApi.selection.legacyGetSelectedRows()[0]);
            const newNotebookName = await this.IgniteInput.clone(clonedNotebook.name, this.getNotebooksNames());

            this.IgniteLoading.start('notebooksLoading');

            await this.IgniteNotebook.clone(newNotebookName, clonedNotebook);

            this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();
        }
        catch (err) {
            this.IgniteMessages.showError(err);
        }
        finally {
            this.IgniteLoading.finish('notebooksLoading');

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

            this.IgniteLoading.finish('notebooksLoading');

            this._loadAllNotebooks();
        }
        catch (err) {
            this.IgniteMessages.showError(err);

            this.IgniteLoading.finish('notebooksLoading');
        }
    }

    _adjustHeight(rows) {
        // Add header height.
        const height = Math.min(rows, this.rowsToShow) * 48 + 78;

        this.gridApi.grid.element.css('height', height + 'px');

        this.gridApi.core.handleWindowResize();
    }
}
