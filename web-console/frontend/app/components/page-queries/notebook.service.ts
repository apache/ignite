

import _ from 'lodash';
import uuidv4 from 'uuid/v4';
import {StateService} from '@uirouter/angularjs';
import {default as ConfirmModalFactory} from 'app/services/Confirm.service';
import {default as MessagesFactory} from 'app/services/Messages.service';
import {default as NotebookData} from './notebook.data';

export default class Notebook {
    static $inject = ['$state', 'IgniteConfirm', 'IgniteMessages', 'IgniteNotebookData', '$translate'];

    constructor(
        private $state: StateService,
        private confirmModal: ReturnType<typeof ConfirmModalFactory>,
        private Messages: ReturnType<typeof MessagesFactory>,
        private NotebookData: NotebookData,
        private $translate: ng.translate.ITranslateService
    ) {}

    read() {
        return this.NotebookData.read();
    }

    create(name) {
        return this.NotebookData.save({id: uuidv4(), name});
    }

    save(notebook) {
        return this.NotebookData.save(notebook);
    }

    async clone(newNotebookName, clonedNotebook) {
        const newNotebook = await this.create(newNotebookName);

        Object.assign(clonedNotebook, {name: newNotebook.name, id: newNotebook.id });

        return this.save(clonedNotebook);
    }

    find(id) {
        return this.NotebookData.find(id);
    }

    _openNotebook(idx) {
        return this.NotebookData.read()
            .then((notebooks) => {
                const nextNotebook = notebooks.length > idx ? notebooks[idx] : _.last(notebooks);

                if (nextNotebook)
                    this.$state.go('base.sql.tabs.notebook', {noteId: nextNotebook.id});
                else
                    this.$state.go('base.sql.tabs.notebooks-list');
            });
    }

    remove(notebook) {
        return this.confirmModal.confirm(this.$translate.instant('queries.removeNotebookConfirmationMessage', {name: notebook.name}))
            .then(() => this.NotebookData.findIndex(notebook))
            .then((idx) => {
                return this.NotebookData.remove(notebook)
                    .then(() => {
                        if (this.$state.includes('base.sql.tabs.notebook') && this.$state.params.noteId === notebook.id)
                            return this._openNotebook(idx);
                    })
                    .catch(this.Messages.showError);
            });
    }

    removeBatch(notebooks) {
        return this.confirmModal.confirm(this.$translate.instant('queries.removeNotebooksConfirmationMessage', {amount: notebooks.length}))
            .then(() => {
                const deleteNotebooksPromises = notebooks.map((notebook) => this.NotebookData.remove(notebook));

                return Promise.all(deleteNotebooksPromises);
            })
            .catch(this.Messages.showError);
    }
}
