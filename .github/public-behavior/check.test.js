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

// Run with: node --test .github/public-behavior/check.test.js

'use strict';

const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { test } = require('node:test');
const check = require('./check.js');

/** @return Declaration line in the format of PublicDeclarations.java. */
const line = (area, key, value, flags = '', location = 'A.java:1') => [area, key, value, flags, location].join('\t');

const BASE = [
    line('CLI', 'control.sh --cache list --groups', 'optional, boolean'),
    line('Monitoring', 'system view CacheView column cacheName', 'position 1, java.lang.String'),
    line('Monitoring', 'system view CacheView column cacheId', 'position 2, int'),
    line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated'),
    line('Java API', 'org.apache.ignite.Exp#run()', 'public void run()', 'experimental'),
].join('\n');

const CONTRACT = { area: 'CLI', key: 'control.sh --cache list --seq', before: null, after: 'optional, boolean', kind: 'contract', location: 'A.java:1' };
const DEPRECATED = { area: 'Java API', key: 'org.apache.ignite.Old#run()', before: 'public void run()', after: null, kind: 'deprecated-removal', location: 'A.java:1' };
const NO_RFC = { files: [], invalidNames: [], linked: [], badLinks: [] };

test('a declaration that only moved in the source is not a change', () => {
    assert.deepStrictEqual(check.compare(BASE, BASE.replace(/A\.java:1/g, 'B.java:7')), []);
});

test('added, removed and changed declarations are found', () => {
    const merge = BASE
        .replace('position 1, java.lang.String', 'position 2, java.lang.String')
        .replace('position 2, int', 'position 1, int')
        .replace(line('CLI', 'control.sh --cache list --groups', 'optional, boolean') + '\n', '')
        + '\n' + line('CLI', 'control.sh --cache list --seq', 'optional, boolean');

    assert.deepStrictEqual(check.compare(BASE, merge).map(f => [f.key, f.before, f.after, f.kind]), [
        ['control.sh --cache list --groups', 'optional, boolean', null, 'contract'],
        ['control.sh --cache list --seq', null, 'optional, boolean', 'contract'],
        ['system view CacheView column cacheId', 'position 2, int', 'position 1, int', 'contract'],
        ['system view CacheView column cacheName', 'position 1, java.lang.String', 'position 2, java.lang.String', 'contract'],
    ]);
});

test('removing a deprecated declaration and changing experimental API are left to the reviewer', () => {
    const merge = BASE
        .replace(line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated') + '\n', '')
        .replace(line('Java API', 'org.apache.ignite.Exp#run()', 'public void run()', 'experimental'),
            line('Java API', 'org.apache.ignite.Exp#run()', 'public int run()', 'experimental'))
        + '\n' + line('Java API', 'org.apache.ignite.Exp#stop()', 'public void stop()', 'experimental');

    assert.deepStrictEqual(check.compare(BASE, merge).map(f => [f.key, f.kind]), [
        ['org.apache.ignite.Exp#run()', 'experimental'],
        ['org.apache.ignite.Exp#stop()', 'experimental'],
        ['org.apache.ignite.Old#run()', 'deprecated-removal'],
    ]);
});

test('deprecating a declaration, changing a deprecated one or marking stable API experimental needs an RFC', () => {
    const base = line('Java API', 'org.apache.ignite.A#run()', 'public void run()');
    const deprecated = line('Java API', 'org.apache.ignite.A#run()', '@Deprecated public void run()', 'deprecated');
    const changedDeprecated = line('Java API', 'org.apache.ignite.A#run()', '@Deprecated public int run()', 'deprecated');
    const experimental = line('Java API', 'org.apache.ignite.A#run()', '@IgniteExperimental public void run()', 'experimental');

    assert.strictEqual(check.compare(base, deprecated)[0].kind, 'contract');
    assert.strictEqual(check.compare(deprecated, changedDeprecated)[0].kind, 'contract');
    assert.strictEqual(check.compare(base, experimental)[0].kind, 'contract');
});

test('RFC files of the PR and RFC links in the description are found', () => {
    const nameStatus = [
        'A\trfcs/IGNITE-1-new-feature.md',
        'M\trfcs/IGNITE-2-updated.md',
        'R100\trfcs/IGNITE-3-old.md\trfcs/IGNITE-4-renamed.md',
        'A\trfcs/my-proposal.md',
        'M\trfcs/TEMPLATE.md',
        'A\trfcs/diagram.png',
        'D\trfcs/IGNITE-5-removed.md',
        '',
    ].join('\n');

    const body = 'Covered by https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-existing.md. ' +
        'See [RFC](https://github.com/apache/ignite/blob/ignite-2.18/rfcs/IGNITE-8-absent.md#compatibility), ' +
        'https://github.com/apache/ignite/blob/master/rfcs/Bad_Name.md and https://github.com/apache/ignite/pull/13760.';

    assert.deepStrictEqual(check.findRfcs(nameStatus, body, f => f === 'rfcs/IGNITE-7-existing.md'), {
        files: ['rfcs/IGNITE-1-new-feature.md', 'rfcs/IGNITE-2-updated.md', 'rfcs/IGNITE-4-renamed.md', 'rfcs/my-proposal.md'],
        invalidNames: ['rfcs/my-proposal.md'],
        linked: ['rfcs/IGNITE-7-existing.md'],
        badLinks: [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }, { path: 'rfcs/Bad_Name.md', reason: 'name' }],
    });
});

test('a PR without contract changes passes before the RFC check', () => {
    assert.deepStrictEqual(check.decide([], { ...NO_RFC, invalidNames: ['rfcs/x.md'], files: ['rfcs/x.md'] }), { outcome: 'clean' });
});

test('a PR that only removes deprecated or changes experimental API passes with a review warning', () => {
    assert.strictEqual(check.decide([DEPRECATED], NO_RFC).outcome, 'review');
});

test('a contract change without an RFC fails and explains the bad links', () => {
    const res = check.decide([CONTRACT, DEPRECATED], { ...NO_RFC, badLinks: [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }] });

    assert.strictEqual(res.outcome, 'rfc-missing');
    assert.deepStrictEqual(res.badLinks, [{ path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' }]);
});

test('an invalid RFC filename fails even with a valid RFC and a valid link', () => {
    const res = check.decide([CONTRACT], {
        files: ['rfcs/IGNITE-1-x.md', 'rfcs/my-proposal.md'], invalidNames: ['rfcs/my-proposal.md'], linked: ['rfcs/IGNITE-7-y.md'], badLinks: [],
    });

    assert.deepStrictEqual([res.outcome, res.invalidNames], ['rfc-name', ['rfcs/my-proposal.md']]);
});

test('an RFC in the PR or a link to an existing RFC passes with a review warning', () => {
    const inPr = check.decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'], linked: ['rfcs/IGNITE-7-y.md'] });
    const linked = check.decide([CONTRACT], { ...NO_RFC, linked: ['rfcs/IGNITE-7-y.md'] });

    assert.deepStrictEqual([inPr.outcome, inPr.rfcs], ['rfc-found', [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr' }]]);
    assert.deepStrictEqual([linked.outcome, linked.rfcs], ['rfc-found', [{ path: 'rfcs/IGNITE-7-y.md', source: 'link' }]]);
});

test('results that the PR job could not produce are rejected', () => {
    const ok = check.decide([CONTRACT], NO_RFC);

    assert.deepStrictEqual(check.validate(ok), ok);
    assert.deepStrictEqual(check.validate({ outcome: 'error', step: 'rm -rf' }), { outcome: 'error', step: 'result' });
    assert.throws(() => check.validate({ outcome: 'approved' }));
    assert.throws(() => check.validate({ ...ok, findings: [{ ...CONTRACT, key: 'x\n### Approved' }] }));
    assert.throws(() => check.validate({ ...ok, outcome: 'rfc-found', rfcs: [{ path: 'rfcs/IGNITE-1-x.md)](https://evil', source: 'pr' }] }));
    assert.throws(() => check.validate({ ...ok, outcome: 'review' }));
    assert.throws(() => check.validate({ ...ok, outcome: 'rfc-found' }));
});

test('PR data cannot escape code formatting', () => {
    const res = {
        outcome: 'rfc-missing', rfcs: [], invalidNames: [],
        findings: [{ ...CONTRACT, key: 'x ``` @someone [link](https://evil)', after: '````' }],
        badLinks: [{ path: 'rfcs/`@someone`.md', reason: 'name' }],
    };
    const md = check.render(res, {}, 100);

    assert.ok(md.includes('`rfcs/@someone.md`'), md);
    assert.ok(md.includes('`````diff\n+ x ``` @someone [link](https://evil): ```` (A.java:1)\n`````'), md);
});

test('long reports are cut to stay within the GitHub comment limit', () => {
    const findings = Array.from({ length: 30 }, (_, i) => ({ ...CONTRACT, key: 'k' + i, after: 'x'.repeat(i === 0 ? 1000 : 1) }));
    const md = check.render({ outcome: 'rfc-missing', findings, rfcs: [], invalidNames: [], badLinks: [] }, {}, 10);

    assert.strictEqual(md.split('\n').find(l => l.startsWith('+ k0')).length, 500);
    assert.ok(md.includes('+ k9: x') && !md.includes('+ k10: x'), md);
    assert.ok(md.includes('20 more changes are not shown here.'), md);
});

/** GitHub API stub that keeps the PR comments. */
function github({ headSha = 'abc', comments = [] } = {}) {
    const calls = [];
    let id = 100;

    const rest = {
        pulls: { list: 'pulls.list', get: async () => ({ data: { number: 7, head: { sha: headSha }, base: { ref: 'master' } } }) },
        issues: {
            listComments: 'issues.listComments',
            deleteComment: async ({ comment_id }) => {
                calls.push('delete ' + comment_id);
                comments.splice(comments.findIndex(c => c.id === comment_id), 1);
            },
            createComment: async ({ body }) => {
                calls.push('create');
                comments.push({ id: id++, body, user: { login: 'github-actions[bot]' } });
            },
        },
    };

    const paginate = async route => route === 'pulls.list' ? [{ number: 7, head: { sha: headSha }, base: { ref: 'master' } }] : comments;

    return { rest, paginate, calls, comments };
}

/** Publishes a result of a run for a fork PR. */
async function publish(gh, res) {
    const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'pbc-')), 'result.json');

    if (res !== undefined)
        fs.writeFileSync(file, typeof res === 'string' ? res : JSON.stringify(res));

    const context = {
        repo: { owner: 'apache', repo: 'ignite' },
        payload: {
            workflow_run: {
                head_sha: 'abc', head_branch: 'ignite-1', pull_requests: [], html_url: 'https://github.com/apache/ignite/actions/runs/1',
                head_repository: { owner: { login: 'fork' }, html_url: 'https://github.com/fork/ignite' },
            },
        },
    };

    await check.publish({ github: gh, context, core: { info() {}, warning() {} }, resultFile: file });
}

test('a clean PR gets no comment', async () => {
    const gh = github();

    await publish(gh, { outcome: 'clean' });

    assert.deepStrictEqual(gh.calls, []);
});

test('the comment is replaced only when the result changes and says when all issues are resolved', async () => {
    const gh = github();
    const missing = check.decide([CONTRACT], NO_RFC);

    await publish(gh, missing);
    await publish(gh, missing);

    assert.deepStrictEqual(gh.calls, ['create']);
    assert.ok(gh.comments[0].body.includes('RFC needed'));

    await publish(gh, check.decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'] }));

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create']);
    assert.ok(gh.comments[0].body.includes('[rfcs/IGNITE-1-x.md](https://github.com/fork/ignite/blob/abc/rfcs/IGNITE-1-x.md)'));

    await publish(gh, { outcome: 'clean' });
    await publish(gh, { outcome: 'clean' });

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create', 'delete 101', 'create']);
    assert.ok(gh.comments[0].body.includes('All earlier issues are resolved.'));
});

test('comments of other users are kept', async () => {
    const gh = github({ comments: [{ id: 1, body: '<!-- ignite-public-behavior-changes', user: { login: 'someone' } }] });

    await publish(gh, check.decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, ['create']);
});

test('a result for an outdated PR head is ignored', async () => {
    const gh = github({ headSha: 'newer' });

    await publish(gh, check.decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, []);
});

test('a missing or forged result is reported as a check that could not finish', async () => {
    for (const res of [undefined, '{"outcome": "rfc-found", "findings": [], "rfcs": [], "invalidNames": [], "badLinks": []}']) {
        const gh = github();

        await publish(gh, res);

        assert.ok(gh.comments[0].body.includes('Check could not finish'), gh.comments[0].body);
    }
});

/** Runs git in a directory. */
const git = (dir, ...args) => execFileSync('git', ['-C', dir, '-c', 'user.name=t', '-c', 'user.email=t@t', ...args], { stdio: 'pipe' });

/**
 * Lays out the PR job workspace: the merge checkout whose last commit adds the given RFC, the target worktree and the
 * descriptions of both builds.
 */
function workspace({ rfc = null, base = BASE, merge = null } = {}) {
    const ws = fs.mkdtempSync(path.join(os.tmpdir(), 'pbc-ws-'));
    const dir = path.join(ws, 'merge');

    fs.mkdirSync(path.join(dir, 'rfcs'), { recursive: true });
    git(dir, 'init', '-q');
    git(dir, 'commit', '-q', '--allow-empty', '-m', 'target');

    if (rfc) {
        fs.writeFileSync(path.join(dir, rfc), 'rfc');
        git(dir, 'add', '.');
    }

    git(dir, 'commit', '-q', '--allow-empty', '-m', 'merge');
    git(dir, 'worktree', 'add', '-q', '--detach', '../base', 'HEAD^1');

    if (merge !== null) {
        fs.writeFileSync(path.join(ws, 'base.txt'), base);
        fs.writeFileSync(path.join(ws, 'merge.txt'), merge);
    }

    return ws;
}

/** Runs report() in the workspace. */
async function report(ws, steps = { checkout: 'success', base: 'success', merge: 'success' }) {
    const out = { summary: '', annotations: [] };
    const core = {
        summary: { addRaw(s) { out.summary = s; return this; }, async write() {} },
        warning: (m, p) => out.annotations.push('warning: ' + (p ? p.title : m)),
        setFailed: m => out.annotations.push('failed: ' + m),
        error: m => out.annotations.push('error: ' + m),
    };
    const context = {
        payload: { pull_request: { body: '', head: { sha: 'abc', repo: { html_url: 'https://github.com/fork/ignite' } }, base: { ref: 'master' } } },
    };

    await check.report({ core, context, steps, workspace: ws });

    return { ...out, result: JSON.parse(fs.readFileSync(path.join(ws, 'result.json'), 'utf8')) };
}

test('report: a contract change with an RFC in the PR passes with a warning', async () => {
    const out = await report(workspace({ rfc: 'rfcs/IGNITE-1-x.md', merge: BASE + '\n' + line('CLI', 'control.sh --cache list --seq', 'optional, boolean') }));

    assert.strictEqual(out.result.outcome, 'rfc-found');
    assert.deepStrictEqual(out.annotations, ['warning: Review the RFC']);
    assert.ok(out.summary.includes('+ control.sh --cache list --seq: optional, boolean (A.java:1)'), out.summary);
});

test('report: a PR without module changes passes quietly', async () => {
    const out = await report(workspace({ rfc: 'rfcs/IGNITE-1-x.md' }), { checkout: 'success', base: 'skipped', merge: 'skipped' });

    assert.deepStrictEqual([out.result, out.annotations], [{ outcome: 'clean' }, []]);
});

test('report: a PR merge that does not build fails as a check that could not finish', async () => {
    const out = await report(workspace(), { checkout: 'success', base: 'success', merge: 'failure' });

    assert.deepStrictEqual(out.result, { outcome: 'error', step: 'merge' });
    assert.ok(out.summary.includes('If the PR does not compile, fix it.'), out.summary);
});

/**
 * Runs report() for a PR that removes a declaration deprecated in the last target commit.
 *
 * @param tag Release tag of that commit, or null.
 * @return Deprecation hint and the deprecating commit.
 */
async function deprecationHint(tag) {
    const ws = workspace();
    const base = path.join(ws, 'base');

    fs.writeFileSync(path.join(base, 'Old.java'), 'class Old {\n    @Deprecated\n    void run() {}\n}\n');
    git(base, 'add', '.');
    git(base, 'commit', '-q', '-m', 'deprecate');

    if (tag)
        git(base, 'tag', tag);

    fs.writeFileSync(path.join(ws, 'base.txt'), line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated', 'Old.java:2'));
    fs.writeFileSync(path.join(ws, 'merge.txt'), '');

    const out = await report(ws);

    assert.strictEqual(out.result.outcome, 'review');

    return { hint: out.result.findings[0].hint, sha: git(base, 'rev-parse', '--short=10', 'HEAD').toString().trim() };
}

test('report: a removed deprecated declaration shows when it was deprecated and the first release', async () => {
    const { hint, sha } = await deprecationHint('2.9.0');

    assert.match(hint, new RegExp(`^deprecated in ${sha} on \\d{4}-\\d\\d-\\d\\d, first release 2\\.9\\.0$`));
});

test('report: without release tags, as in a fork, the hint names no release', async () => {
    const { hint, sha } = await deprecationHint(null);

    assert.match(hint, new RegExp(`^deprecated in ${sha} on \\d{4}-\\d\\d-\\d\\d$`));
});
