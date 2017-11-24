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

export default class Notebook {
    static $inject = ['$state', 'IgniteConfirm', 'IgniteMessages', 'IgniteNotebookData'];

    /**
     * @param $state
     * @param confirmModal
     * @param Messages
     * @param {NotebookData} NotebookData
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

    find(_id) {
        return this.NotebookData.find(_id);
    }

    _openNotebook(idx) {
        return this.NotebookData.read()
            .then((notebooks) => {
                const nextNotebook = notebooks.length > idx ? notebooks[idx] : _.last(notebooks);

                if (nextNotebook)
                    this.$state.go('base.sql.notebook', {noteId: nextNotebook._id});
                else
                    this.$state.go('base.configuration.clusters');
            });
    }

    remove(notebook) {
        return this.confirmModal.confirm(`Are you sure you want to remove notebook: "${notebook.name}"?`)
            .then(() => this.NotebookData.findIndex(notebook))
            .then((idx) => {
                this.NotebookData.remove(notebook)
                    .then(() => {
                        if (this.$state.includes('base.sql.notebook') && this.$state.params.noteId === notebook._id)
                            return this._openNotebook(idx);
                    })
                    .catch(this.Messages.showError);
            });
    }
}
