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

'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { test } = require('node:test');
const { publish } = require('./publish.js');
const { decide } = require('./outcome.js');
const { NO_RFC } = require('./rfc.js');
const { CONTRACT, APACHE } = require('./test-fixtures.js');

/**
 * GitHub API stub that keeps the PR comments.
 *
 * @param headSha Head commit of the open PR.
 * @param comments Comments that the PR already has.
 * @param runs Check runs for the head commit: number and PR branch.
 * @param prs Open PRs from the checked branch: number and target branch.
 */
function github({
    headSha = 'abc', comments = [], runs = [{ number: 1, branch: 'ignite-1' }], prs = [{ number: 7, base: 'master' }],
} = {}) {
    const calls = [];
    const commented = [];
    let id = 100;

    const rest = {
        pulls: { list: 'pulls.list' },
        actions: {
            listWorkflowRuns: async () => ({
                data: {
                    workflow_runs: runs.map(r => ({ run_number: r.number, head_branch: r.branch, head_repository: { full_name: 'fork/ignite' } })),
                },
            }),
        },
        issues: {
            listComments: 'issues.listComments',
            deleteComment: async ({ comment_id }) => {
                calls.push('delete ' + comment_id);
                comments.splice(comments.findIndex(c => c.id === comment_id), 1);
            },
            createComment: async ({ issue_number, body }) => {
                calls.push('create');
                commented.push(issue_number);
                comments.push({ id: id++, body, user: { login: 'github-actions[bot]' } });
            },
        },
    };

    const paginate = async (route, params) => route !== 'pulls.list' ? comments
        : prs.filter(p => p.base === params.base)
            .map(p => ({ number: p.number, head: { sha: headSha }, base: { ref: p.base, repo: APACHE } }));

    return { rest, paginate, calls, comments, commented };
}

/** Publishes the result of check run 1 for a fork PR. */
async function publishResult(gh, res) {
    const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'pbc-')), 'result.json');

    if (res !== undefined)
        fs.writeFileSync(file, typeof res === 'string' ? res : JSON.stringify(res));

    const context = {
        repo: { owner: 'apache', repo: 'ignite' },
        payload: {
            workflow_run: {
                head_sha: 'abc', head_branch: 'ignite-1', pull_requests: [], html_url: 'https://github.com/apache/ignite/actions/runs/1',
                head_repository: { owner: { login: 'fork' }, full_name: 'fork/ignite', html_url: 'https://github.com/fork/ignite' },
                workflow_id: 5, run_number: 1,
            },
        },
    };

    await publish({ github: gh, context, core: { info() {}, warning() {} }, resultFile: file, target: 'master' });
}

test('a clean PR gets no comment', async () => {
    const gh = github();

    await publishResult(gh, { outcome: 'clean' });

    assert.deepStrictEqual(gh.calls, []);
});

test('the comment is replaced only when the result changes and says when all issues are resolved', async () => {
    const gh = github();
    const missing = decide([CONTRACT], NO_RFC);

    await publishResult(gh, missing);
    await publishResult(gh, missing);

    assert.deepStrictEqual(gh.calls, ['create']);
    assert.ok(gh.comments[0].body.includes('RFC needed'));

    await publishResult(gh, decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'] }));

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create']);
    assert.ok(gh.comments[0].body.includes('[rfcs/IGNITE-1-x.md](https://github.com/fork/ignite/blob/abc/rfcs/IGNITE-1-x.md)'));

    await publishResult(gh, { outcome: 'clean' });
    await publishResult(gh, { outcome: 'clean' });

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create', 'delete 101', 'create']);
    assert.ok(gh.comments[0].body.includes('All earlier issues are resolved.'));
});

test('a new version of the RFC text replaces the comment, so its link leads to the current text', async () => {
    const gh = github();
    const found = decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'] });
    const withBlob = blob => ({ ...found, rfcs: [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr', blob }] });

    await publishResult(gh, withBlob('a'.repeat(40)));
    await publishResult(gh, withBlob('a'.repeat(40)));
    await publishResult(gh, withBlob('b'.repeat(40)));

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create']);
});

test('comments of other users are kept', async () => {
    const gh = github({ comments: [{ id: 1, body: '<!-- ignite-public-behavior-changes', user: { login: 'someone' } }] });

    await publishResult(gh, decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, ['create']);
});

test('a result for an outdated PR head is ignored', async () => {
    const gh = github({ headSha: 'newer' });

    await publishResult(gh, decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, []);
});

test('a result is ignored when a newer run checks the same commit of the same PR', async () => {
    const gh = github({ runs: [{ number: 1, branch: 'ignite-1' }, { number: 2, branch: 'ignite-1' }] });

    await publishResult(gh, decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, []);
});

test('a newer run of another PR with the same commit does not suppress the result', async () => {
    const gh = github({ runs: [{ number: 1, branch: 'ignite-1' }, { number: 2, branch: 'ignite-1-copy' }] });

    await publishResult(gh, decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, ['create']);
});

test('the comment goes to the PR into the target branch, not to a PR of the same branch into a release branch', async () => {
    const gh = github({ prs: [{ number: 8, base: 'ignite-2.17' }, { number: 7, base: 'master' }] });

    await publishResult(gh, decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.commented, [7]);
});

test('a missing or forged result is reported as a check that could not finish', async () => {
    for (const res of [undefined, '{"outcome": "rfc-found", "findings": [], "rfcs": [], "invalidNames": [], "badLinks": []}']) {
        const gh = github();

        await publishResult(gh, res);

        assert.ok(gh.comments[0].body.includes('Check could not finish'), gh.comments[0].body);
    }
});
