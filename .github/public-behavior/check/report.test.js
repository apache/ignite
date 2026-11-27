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
const { execFileSync } = require('child_process');
const { test } = require('node:test');
const check = require('./report.js');
const { property, BASE, APACHE } = require('./test-fixtures.js');

/** Runs git in a directory. */
const git = (dir, ...args) => execFileSync('git', ['-C', dir, '-c', 'user.name=t', '-c', 'user.email=t@t', ...args], { stdio: 'pipe' });

/** Writes files of a checkout, by path. */
function write(dir, files) {
    for (const [f, text] of Object.entries(files)) {
        fs.mkdirSync(path.dirname(path.join(dir, f)), { recursive: true });
        fs.writeFileSync(path.join(dir, f), text);
    }
}

/**
 * Lays out the PR job workspace: the merge checkout whose last commit adds the given files, the target worktree and the
 * descriptions of both builds.
 *
 * @param files Files that the PR adds or changes, by path.
 * @param removed Files of the target branch that the PR removes.
 * @param onTarget Files of the target branch, by path.
 * @param merge Description of the PR merge, or null if the builds were skipped.
 */
function workspace({ files = {}, removed = [], onTarget = {}, merge = null } = {}) {
    const ws = fs.mkdtempSync(path.join(os.tmpdir(), 'pbc-ws-'));
    const dir = path.join(ws, 'merge');

    fs.mkdirSync(path.join(dir, 'rfcs'), { recursive: true });
    git(dir, 'init', '-q');
    write(dir, onTarget);
    git(dir, 'add', '.');
    git(dir, 'commit', '-q', '--allow-empty', '-m', 'target');
    write(dir, files);
    removed.forEach(f => fs.rmSync(path.join(dir, f)));
    git(dir, 'add', '-A');
    git(dir, 'commit', '-q', '--allow-empty', '-m', 'merge');
    git(dir, 'worktree', 'add', '-q', '--detach', '../base', 'HEAD^1');

    if (merge !== null) {
        fs.writeFileSync(path.join(ws, 'base.txt'), BASE);
        fs.writeFileSync(path.join(ws, 'merge.txt'), merge);
    }

    return ws;
}

const BUILT = { checkout: 'success', tests: 'success', java: 'success', base: 'success', merge: 'success' };
const SKIPPED = { checkout: 'success', tests: 'success', java: 'skipped', base: 'skipped', merge: 'skipped' };
const WITH_CONTRACT_CHANGE = BASE + '\n' + property('IGNITE_NEW', 'Boolean, default false');

/**
 * Runs report() in the workspace.
 *
 * @param body PR description.
 * @param repo Target repository of the PR.
 * @return Title of the outcome, the job summary and the annotations.
 */
async function report(ws, { steps = BUILT, body = '', repo = APACHE } = {}) {
    const out = { summary: '', annotations: [] };
    const core = {
        summary: { addRaw(s) { out.summary = s; return this; }, async write() {} },
        warning: (m, p) => out.annotations.push('warning: ' + (p ? p.title : m)),
        setFailed: m => out.annotations.push('failed: ' + m),
        error: m => out.annotations.push('error: ' + m),
    };
    const context = {
        payload: {
            pull_request: {
                body, head: { sha: 'abc', repo: { html_url: 'https://github.com/fork/ignite' } }, base: { ref: 'master', repo },
            },
        },
    };

    await check.report({ core, context, steps, workspace: ws });

    return { ...out, title: out.summary.split('\n')[0].replace('### Public behavior changes: ', '') };
}

/** @return Paths of the RFC files that the summary links. */
const linkedRfcs = summary => [...(summary.match(/^RFC: .*$/m) || [''])[0].matchAll(/\[([^\]]+)\]/g)].map(m => m[1]);

test('report: a contract change with an RFC in the PR passes with a warning and links the RFC in the PR', async () => {
    const out = await report(workspace({ files: { 'rfcs/IGNITE-1-x.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE }));

    assert.deepStrictEqual(out.annotations, ['warning: Review the RFC']);
    assert.ok(out.summary.includes('[rfcs/IGNITE-1-x.md](https://github.com/fork/ignite/blob/abc/rfcs/IGNITE-1-x.md)'), out.summary);
    assert.ok(out.summary.includes('+ system property IGNITE_NEW: Boolean, default false (A.java:1)'), out.summary);
});

test('report: the RFC files of the PR are the ones it adds, updates or renames to', async () => {
    const onTarget = {
        'rfcs/IGNITE-2-updated.md': 'v1', 'rfcs/IGNITE-3-old.md': 'old', 'rfcs/IGNITE-5-removed.md': 'gone', 'rfcs/TEMPLATE.md': 't',
    };
    const files = {
        'rfcs/IGNITE-1-new-feature.md': 'new', 'rfcs/IGNITE-2-updated.md': 'v2', 'rfcs/IGNITE-4-renamed.md': 'old',
        'rfcs/TEMPLATE.md': 't2', 'rfcs/diagram.png': 'png',
    };
    const ws = workspace({ onTarget, files, removed: ['rfcs/IGNITE-3-old.md', 'rfcs/IGNITE-5-removed.md'], merge: WITH_CONTRACT_CHANGE });
    const out = await report(ws);

    assert.deepStrictEqual([out.title, linkedRfcs(out.summary)],
        ['Review the RFC', ['rfcs/IGNITE-1-new-feature.md', 'rfcs/IGNITE-2-updated.md', 'rfcs/IGNITE-4-renamed.md']]);
});

test('report: an RFC file whose name git would quote fails with the filename error', async () => {
    const out = await report(workspace({ files: { 'rfcs/обзор.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE }));

    assert.strictEqual(out.title, 'Fix the RFC filename');
    assert.ok(out.summary.includes('- `rfcs/обзор.md` does not match'), out.summary);
});

test('report: a link counts only to the target repository and only if its file is on the target branch', async () => {
    const stand = { owner: { login: 'owner' }, name: 'stand', full_name: 'owner/stand', html_url: 'https://github.com/owner/stand' };
    const onTarget = { 'rfcs/IGNITE-7-y.md': 'rfc' };
    const missing = await report(workspace({ onTarget, merge: WITH_CONTRACT_CHANGE }), { repo: stand,
        body: 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md https://github.com/owner/stand/blob/mastr/rfcs/IGNITE-8-z.md' });
    const found = await report(workspace({ onTarget, merge: WITH_CONTRACT_CHANGE }), { repo: stand,
        body: 'https://github.com/owner/stand/blob/mastr/rfcs/IGNITE-7-y.md' });

    assert.strictEqual(missing.title, 'RFC needed');
    assert.deepStrictEqual(missing.annotations, ['failed: RFC needed. See the job summary.']);
    assert.ok(missing.summary.includes('- `rfcs/IGNITE-8-z.md` is not on the target branch.'), missing.summary);
    assert.deepStrictEqual([found.title, linkedRfcs(found.summary)], ['Review the RFC', ['rfcs/IGNITE-7-y.md']]);
    assert.ok(found.summary.includes('(https://github.com/owner/stand/blob/master/rfcs/IGNITE-7-y.md)'), found.summary);
});

test('report: a PR that only removes a deprecated declaration passes with a warning and needs no RFC', async () => {
    const merge = BASE.replace(/\n.*IGNITE_EVICTION_PERMITS.*/, '');
    const out = await report(workspace({ files: { 'modules/core/IgniteSystemProperties.java': 'v2' }, merge }));

    assert.deepStrictEqual(out.annotations, ['warning: Review the removed deprecated declarations']);
});

test('report: a PR that changes only the check shows what the check describes differently and needs no RFC', async () => {
    const files = {
        '.github/public-behavior/describe/src/collectors/SystemProperties.java': 'v2', '.github/workflows/public-behavior-changes.yml': 'v2',
    };
    const out = await report(workspace({ files, merge: WITH_CONTRACT_CHANGE }));

    assert.deepStrictEqual(out.annotations, ['warning: Review the changes to the check']);
    assert.ok(out.summary.includes('+ system property IGNITE_NEW: Boolean, default false (A.java:1)'), out.summary);
});

test('report: a PR that changes the check and Ignite code needs an RFC for its changes', async () => {
    const files = { '.github/public-behavior/describe/src/collectors/SystemProperties.java': 'v2', 'modules/core/IgniteSystemProperties.java': 'v2' };
    const out = await report(workspace({ files, merge: WITH_CONTRACT_CHANGE }));

    assert.strictEqual(out.title, 'RFC needed');
});

test('report: a PR without public changes passes quietly, whether the builds ran or not', async () => {
    const skipped = await report(workspace({ files: { 'rfcs/IGNITE-1-x.md': 'rfc' } }), { steps: SKIPPED });
    const built = await report(workspace({ files: { 'modules/core/A.java': 'v2' }, merge: BASE.replace(/A\.java:1/g, 'A.java:2') }));

    assert.deepStrictEqual([skipped.title, skipped.annotations], ['No public behavior changes', []]);
    assert.deepStrictEqual([built.title, built.annotations], ['No public behavior changes', []]);
});

test('report: failing tests of the check fail the check and ask to fix the check', async () => {
    const out = await report(workspace(), { steps: { ...SKIPPED, tests: 'failure' } });

    assert.deepStrictEqual(out.annotations, ['failed: Check could not finish. See the job summary.']);
    assert.ok(out.summary.includes('The tests of the check fail.'), out.summary);
});

test('report: a failed Java setup fails as a check that could not finish, not as a clean PR', async () => {
    const out = await report(workspace(), { steps: { ...SKIPPED, java: 'failure' } });

    assert.ok(out.summary.includes('could not set up Java'), out.summary);
});

test('report: a PR merge that does not build fails as a check that could not finish', async () => {
    const out = await report(workspace(), { steps: { ...BUILT, merge: 'failure' } });

    assert.strictEqual(out.title, 'Check could not finish');
    assert.ok(out.summary.includes('If the PR does not compile, fix it.'), out.summary);
});
