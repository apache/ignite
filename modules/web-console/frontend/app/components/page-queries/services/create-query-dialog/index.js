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

import template from './template.pug';

export default class {
    static $inject = ['$modal'];

    constructor($modal) {
        Object.assign(this, { $modal });
    }

    show() {
        const self = this;

        self.modal = this.$modal({
            template,
            backdrop: 'static',
            controller: class {
                static $inject = ['$state', 'IgniteMessages', 'IgniteNotebook'];

                constructor($state, Messages, Notebook) {
                    Object.assign(this, { $state, Messages, Notebook });
                }

                submit(name) {
                    return this.Notebook.create(name)
                        .then((notebook) => {
                            self.modal.hide();
                            this.$state.go('base.sql.notebook', {noteId: notebook._id});
                        })
                        .catch(this.Messages.showError);
                }
            },
            controllerAs: '$ctrl'
        });
    }

    hide() {
        this.modal && this.modal.hide();
    }
}
