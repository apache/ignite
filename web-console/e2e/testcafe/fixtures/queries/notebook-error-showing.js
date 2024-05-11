/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {WebSocketHook} from '../../mocks/WebSocketHook';
import {
    cacheNamesCollectorTask, agentStat, simeplFakeSQLQuery,
    FAKE_CLUSTERS, SIMPLE_FAILED_QUERY_RESPONSE, FAKE_CACHES
} from '../../mocks/agentTasks';
import {resolveUrl, dropTestDB, insertTestUser} from '../../environment/envtools';
import {createRegularUser} from '../../roles';
import {Paragraph, showStacktraceDialog} from '../../page-models/pageQueryNotebook';
import {PageQueriesNotebooksList} from '../../page-models/PageQueries';

const user = createRegularUser();

fixture('Notebook')
    .beforeEach(async(t) => {
        await dropTestDB();
        await insertTestUser();
        await t.addRequestHooks(
            t.ctx.ws = new WebSocketHook()
                .use(
                    agentStat(FAKE_CLUSTERS),
                    cacheNamesCollectorTask(FAKE_CACHES),
                    simeplFakeSQLQuery(FAKE_CLUSTERS.clusters[0].nids[0], SIMPLE_FAILED_QUERY_RESPONSE)
                )
        );
    })
    .afterEach(async(t) => {
        t.ctx.ws.destroy();
        await dropTestDB();
    });

test('Show stack trace on query failure.', async(t) => {
    const notebooks = new PageQueriesNotebooksList();
    const query = `SELECT * FROM Person;`;
    const paragraph = new Paragraph('Query');

    await t
		.useRole(user)
        .navigateTo(resolveUrl('/queries/notebooks'));
    await notebooks.createNotebook('Foo');
    await t.click(notebooks.getNotebookByName('Foo'));
    await paragraph.enterQuery(query, {replace: true});
    await t
        .click(paragraph.bottomExecuteButton)
        .pressKey('pagedown')
        .click(paragraph.showStacktraceButton)
        .expect(showStacktraceDialog.rootCause.innerText).contains('Outer error message')
        .click(showStacktraceDialog.rootCause)
        .expect(showStacktraceDialog.rootCauseFirstStacktraceLine.innerText).contains('Outer error trace 1')
        .click(showStacktraceDialog.causeWithoutStacktrace)
        .expect(showStacktraceDialog.downloadLink.exists).ok()
        .click(showStacktraceDialog.okButton);
});
