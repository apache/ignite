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
const { line, BASE, APACHE } = require('./test-fixtures.js');

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
 * @param files Files that the PR adds, by path.
 * @param onTarget Files of the target branch, by path.
 * @param merge Description of the PR merge, or null if the builds were skipped.
 */
function workspace({ files = {}, removed = [], onTarget = {}, base = BASE, merge = null } = {}) {
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
        fs.writeFileSync(path.join(ws, 'base.txt'), base);
        fs.writeFileSync(path.join(ws, 'merge.txt'), merge);
    }

    return ws;
}

const BUILT = { checkout: 'success', tests: 'success', java: 'success', base: 'success', merge: 'success' };
const SKIPPED = { checkout: 'success', tests: 'success', java: 'skipped', base: 'skipped', merge: 'skipped' };
const WITH_CONTRACT_CHANGE = BASE + '\n' + line('CLI', 'control.sh --cache list --seq', 'optional, boolean');

/**
 * Runs report() in the workspace.
 *
 * @param body PR description.
 * @param repo Target repository of the PR.
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

    return { ...out, result: JSON.parse(fs.readFileSync(path.join(ws, 'result.json'), 'utf8')) };
}

test('report: a contract change with an RFC in the PR passes with a warning and names the RFC version', async () => {
    const ws = workspace({ files: { 'rfcs/IGNITE-1-x.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE });
    const out = await report(ws);

    assert.strictEqual(out.result.outcome, 'rfc-found');
    assert.strictEqual(out.result.rfcs[0].blob, git(path.join(ws, 'merge'), 'rev-parse', 'HEAD:rfcs/IGNITE-1-x.md').toString().trim());
    assert.deepStrictEqual(out.annotations, ['warning: Review the RFC']);
    assert.ok(out.summary.includes('+ control.sh --cache list --seq: optional, boolean (A.java:1)'), out.summary);
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

    assert.deepStrictEqual([out.result.outcome, out.result.rfcs.map(r => r.path)],
        ['rfc-found', ['rfcs/IGNITE-1-new-feature.md', 'rfcs/IGNITE-2-updated.md', 'rfcs/IGNITE-4-renamed.md']]);
});

test('report: all RFC names are checked, and only the shown list is cut', async () => {
    const files = Object.fromEntries(Array.from({ length: 21 }, (_, i) => [`rfcs/IGNITE-${100 + i}-feature.md`, 'rfc']));
    const valid = await report(workspace({ files, merge: WITH_CONTRACT_CHANGE }));
    const lastInvalid = await report(workspace({ files: { ...files, 'rfcs/zz-proposal.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE }));

    assert.deepStrictEqual([valid.result.outcome, valid.result.rfcs.length, valid.result.notShown], ['rfc-found', 20, 1]);
    assert.match(valid.summary, /IGNITE-119-feature\.md\) and 1 more\./);
    assert.deepStrictEqual([lastInvalid.result.outcome, lastInvalid.result.invalidNames], ['rfc-name', ['rfcs/zz-proposal.md']]);
});

test('report: an RFC file whose name git would quote fails with the filename error', async () => {
    const out = await report(workspace({ files: { 'rfcs/обзор.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE }));

    assert.deepStrictEqual([out.result.outcome, out.result.invalidNames], ['rfc-name', ['rfcs/обзор.md']]);
});

test('report: a link counts only to the target repository and only if its file is on the target branch', async () => {
    const stand = { owner: { login: 'owner' }, name: 'stand', full_name: 'owner/stand', html_url: 'https://github.com/owner/stand' };
    const onTarget = { 'rfcs/IGNITE-7-y.md': 'rfc' };
    const missing = await report(workspace({ onTarget, merge: WITH_CONTRACT_CHANGE }), { repo: stand,
        body: 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md https://github.com/owner/stand/blob/mastr/rfcs/IGNITE-8-z.md' });
    const found = await report(workspace({ onTarget, merge: WITH_CONTRACT_CHANGE }), { repo: stand,
        body: 'https://github.com/owner/stand/blob/mastr/rfcs/IGNITE-7-y.md' });

    assert.deepStrictEqual([missing.result.outcome, missing.result.badLinks], ['rfc-missing', [{ path: 'rfcs/IGNITE-8-z.md', reason: 'missing' }]]);
    assert.deepStrictEqual([found.result.outcome, found.result.rfcs.map(r => [r.path, r.source])], ['rfc-found', [['rfcs/IGNITE-7-y.md', 'link']]]);
});

test('report: a PR that changes only the check shows what the check describes differently and needs no RFC', async () => {
    const files = { '.github/public-behavior/describe/Cli.java': 'v2', '.github/workflows/public-behavior-changes.yml': 'v2' };
    const out = await report(workspace({ files, merge: WITH_CONTRACT_CHANGE }));

    assert.deepStrictEqual([out.result.outcome, out.result.findings.map(f => f.key)], ['check', ['control.sh --cache list --seq']]);
    assert.deepStrictEqual(out.annotations, ['warning: Review the changes to the check']);
});

test('report: a PR that changes the check and Ignite code needs an RFC for its changes', async () => {
    const files = { '.github/public-behavior/describe/Cli.java': 'v2', 'modules/core/Command.java': 'v2' };
    const out = await report(workspace({ files, merge: WITH_CONTRACT_CHANGE }));

    assert.strictEqual(out.result.outcome, 'rfc-missing');
});

test('report: a PR without source changes passes quietly', async () => {
    const out = await report(workspace({ files: { 'rfcs/IGNITE-1-x.md': 'rfc' } }), { steps: SKIPPED });

    assert.deepStrictEqual([out.result, out.annotations], [{ outcome: 'clean' }, []]);
});

test('report: failing tests of the check fail the check and ask to fix the check', async () => {
    const out = await report(workspace(), { steps: { ...SKIPPED, tests: 'failure' } });

    assert.deepStrictEqual(out.result, { outcome: 'error', step: 'tests' });
    assert.ok(out.summary.includes('The tests of the check fail.'), out.summary);
});

test('report: a failed Java setup fails as a check that could not finish, not as a clean PR', async () => {
    const out = await report(workspace(), { steps: { ...SKIPPED, java: 'failure' } });

    assert.deepStrictEqual(out.result, { outcome: 'error', step: 'java' });
    assert.ok(out.summary.includes('could not set up Java'), out.summary);
});

test('report: a PR merge that does not build fails as a check that could not finish', async () => {
    const out = await report(workspace(), { steps: { ...BUILT, merge: 'failure' } });

    assert.deepStrictEqual(out.result, { outcome: 'error', step: 'merge' });
    assert.ok(out.summary.includes('If the PR does not compile, fix it.'), out.summary);
});

/**
 * Runs report() for a PR that removes a declaration deprecated in the last target commit. The declaration itself is older.
 *
 * @param tag Release tag of that commit, or null.
 * @return Deprecation hint and the deprecating commit.
 */
async function deprecationHint(tag) {
    const ws = workspace();
    const base = path.join(ws, 'base');

    fs.writeFileSync(path.join(base, 'Old.java'), 'class Old {\n    void run() {}\n}\n');
    git(base, 'add', '.');
    git(base, 'commit', '-q', '-m', 'add');
    fs.writeFileSync(path.join(base, 'Old.java'), 'class Old {\n    @Deprecated\n    void run() {}\n}\n');
    git(base, 'commit', '-q', '-am', 'deprecate');

    if (tag)
        git(base, 'tag', tag);

    fs.writeFileSync(path.join(ws, 'base.txt'),
        line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated', 'Old.java:3', '', 'Old.java:2'));
    fs.writeFileSync(path.join(ws, 'merge.txt'), line('Java API', 'org.apache.ignite.Other#run()', 'public void run()'));

    const out = await report(ws);

    return { out, sha: git(base, 'rev-parse', '--short=10', 'HEAD').toString().trim() };
}

test('report: a removed deprecated declaration shows when it was deprecated and the first release', async () => {
    const { out, sha } = await deprecationHint('2.9.0');
    const removed = out.result.findings.find(f => f.kind === 'deprecated-removal');

    assert.match(removed.hint, new RegExp(`^deprecated in ${sha} on \\d{4}-\\d\\d-\\d\\d, first release 2\\.9\\.0$`));
});

test('report: without release tags, as in a fork, the hint names no release', async () => {
    const { out, sha } = await deprecationHint(null);
    const removed = out.result.findings.find(f => f.kind === 'deprecated-removal');

    assert.match(removed.hint, new RegExp(`^deprecated in ${sha} on \\d{4}-\\d\\d-\\d\\d$`));
});
