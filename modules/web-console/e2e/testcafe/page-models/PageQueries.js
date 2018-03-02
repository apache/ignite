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


import { Selector, t } from 'testcafe';
import { ModalInput } from '../components/modalInput';
import { confirmation } from '../components/confirmation';
import { mouseenterTrigger } from '../helpers';
import _ from 'lodash';

export class PageQueriesNotebooksList {
    constructor() {
        this.createNotebookButton = Selector('#createNotebookBtn');
        this.createNotebookModal = new ModalInput();
    }

    async createNotebook(notebookName) {
        await t.click(this.createNotebookButton);

        await this.createNotebookModal.enterValue(notebookName);

        return this.createNotebookModal.confirm();
    }

    async selectNotebookByName(notebookName) {
        const notebookRows = await Selector('.notebook-name a');
        const notebookRowsIndices = _.range(await notebookRows.count + 1);
        const notebookRowIndex = notebookRowsIndices.findIndex(async(i) => {
            return notebookName === await notebookRows.nth(i).innerText;
        });

        return t.click(Selector('.ui-grid-selection-row-header-buttons').nth(notebookRowIndex + 1).parent());
    }

    selectAllNotebooks() {
        return t.click(Selector('.ui-grid-selection-row-header-buttons').nth(0).parent());
    }

    async deleteAllNotebooks() {
        await this.selectAllNotebooks();

        await mouseenterTrigger('.btn-ignite:contains(Actions)');
        await t.click(Selector('a').withText('Delete'));

        return confirmation.confirm();
    }

    async cloneNotebook(notebookName) {
        await this.selectNotebookByName(notebookName);
        await mouseenterTrigger('.btn-ignite:contains(Actions)');
        await t.click(Selector('a').withText('Clone'));

        return this.createNotebookModal.confirm();
    }
}
