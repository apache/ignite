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

import { dropTestDB, insertTestUser, resolveUrl } from '../../envtools';
import { createRegularUser } from '../../roles';
import { PageQueriesNotebooksList } from '../../page-models/PageQueries';

const regularUser = createRegularUser();

fixture.only('Checking Ignite queries notebooks list')
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
    const notebookName = 'test_notebook';
    const notebooksListPage = new PageQueriesNotebooksList();

    await notebooksListPage.createNotebook(notebookName);

    await t.expect(notebooksListPage.notebookListTable.findCell(0, 'Name').textContent).contains(notebookName);
});
