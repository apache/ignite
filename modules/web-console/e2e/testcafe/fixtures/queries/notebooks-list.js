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
import { Selector } from 'testcafe';
import { dropTestDB, insertTestUser, resolveUrl } from '../../envtools';
import { createRegularUser } from '../../roles';
import { PageQueriesNotebooksList } from '../../page-models/PageQueries';

const regularUser = createRegularUser();
const notebooksListPage = new PageQueriesNotebooksList();
const notebookName = 'test_notebook';

fixture('Checking Ignite queries notebooks list')
    .before(async() => {
        await dropTestDB();
        await insertTestUser();
    })
    .beforeEach(async(t) => {
        await t.useRole(regularUser);
        await t.navigateTo(resolveUrl('/queries/notebooks'));
    })
    .after(async() => {
        await dropTestDB();
    });

test('Testing creating notebook', async(t) => {
    await notebooksListPage.createNotebook(notebookName);

    await t.expect(Selector('.notebook-name a').withText(notebookName).exists).ok();
});

test('Testing cloning notebook', async(t) => {
    await notebooksListPage.cloneNotebook(notebookName);

    await t.expect(Selector('.notebook-name a').withText(`${notebookName}_1`).exists).ok();
});

test.skip('Testing forbidding of creating notebook with existing name', async(t) => {
    // Todo: Implement this test after investigation on reason of testcafe blocking notebooks API query.
});

test('Testing deleting notebooks', async(t) => {
    await notebooksListPage.deleteAllNotebooks();

    await t.expect(Selector('.notebook-name a').withText(`${notebookName}_1`).exists).notOk();
});
