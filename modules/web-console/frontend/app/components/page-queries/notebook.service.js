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

export default class Notebook {
    static $inject = ['$state', 'IgniteConfirm', 'IgniteMessages', 'IgniteNotebookData'];

    /**
     * @param {import('@uirouter/angularjs').StateService} $state
     * @param {ReturnType<typeof import('app/services/Confirm.service').default>} confirmModal
     * @param {ReturnType<typeof import('app/services/Messages.service').default>} Messages
     * @param {import('./notebook.data').default} NotebookData
     */
    constructor($state, confirmModal, Messages, NotebookData) {
        this.$state = $state;
        this.confirmModal = confirmModal;
        this.Messages = Messages;
        this.NotebookData = NotebookData;
    }

    read() {
        return this.NotebookData.read();
    }

    create(name) {
        return this.NotebookData.save({name});
    }

    save(notebook) {
        return this.NotebookData.save(notebook);
    }

    async clone(newNotebookName, clonedNotebook) {
        const newNotebook = await this.create(newNotebookName);
        Object.assign(clonedNotebook, {name: newNotebook.name, _id: newNotebook._id });

        return this.save(clonedNotebook);
    }

    find(_id) {
        return this.NotebookData.find(_id);
    }

    _openNotebook(idx) {
        return this.NotebookData.read()
            .then((notebooks) => {
                const nextNotebook = notebooks.length > idx ? notebooks[idx] : _.last(notebooks);

                if (nextNotebook)
                    this.$state.go('base.sql.tabs.notebook', {noteId: nextNotebook._id});
                else
                    this.$state.go('base.sql.tabs.notebooks-list');
            });
    }

    remove(notebook) {
        return this.confirmModal.confirm(`Are you sure you want to remove notebook: "${notebook.name}"?`)
            .then(() => this.NotebookData.findIndex(notebook))
            .then((idx) => {
                return this.NotebookData.remove(notebook)
                    .then(() => {
                        if (this.$state.includes('base.sql.tabs.notebook') && this.$state.params.noteId === notebook._id)
                            return this._openNotebook(idx);
                    })
                    .catch(this.Messages.showError);
            });
    }

    removeBatch(notebooks) {
        return this.confirmModal.confirm(`Are you sure you want to remove ${notebooks.length} selected notebooks?`)
            .then(() => {
                const deleteNotebooksPromises = notebooks.map((notebook) => this.NotebookData.remove(notebook));

                return Promise.all(deleteNotebooksPromises);
            })
            .catch(this.Messages.showError);
    }
}
