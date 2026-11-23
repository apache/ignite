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
const line = (area, key, value, flags = '', location = 'A.java:1', group = '', deprecation = '') =>
    [area, key, value, flags, location, group, deprecation].join('\t');

/** Target repository of the PRs in the tests. */
const APACHE = { owner: { login: 'apache' }, name: 'ignite', full_name: 'apache/ignite', html_url: 'https://github.com/apache/ignite' };

/** @return Line of an internal constant {@code Class.NAME}, whose name is a rename group. */
const constant = (area, key, value) => line(area, key, value, '', 'A.java:1', key.split('.').pop());

/** @return Line of a system view column, whose row class is a rename group. */
const column = (view, name, value) => line('Monitoring', `system view ${view} column ${name}`, value, '', 'A.java:1', view);

const BASE = [
    line('CLI', 'control.sh --cache list --groups', 'optional, boolean'),
    column('CacheView', 'cacheName', 'position 1, java.lang.String'),
    column('CacheView', 'cacheId', 'position 2, int'),
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

test('renaming an internal constant or a view row class is not a change', () => {
    const base = [constant('REST', 'GridRestCommand.CACHE_GET', 'get'), BASE].join('\n');
    const merge = [constant('REST', 'GridRestCommand.CACHE_GET_VALUE', 'get'), BASE.replace(/CacheView/g, 'CachesView')].join('\n');

    assert.deepStrictEqual(check.compare(base, merge), []);
});

test('two operations that swap their codes are reported, and so is a renamed view with a moved column', () => {
    const base = [constant('Thin client protocol', 'Parser.OP_GET', '1000'), constant('Thin client protocol', 'Parser.OP_PUT', '1001'), BASE].join('\n');
    const merge = [constant('Thin client protocol', 'Parser.OP_GET', '1001'), constant('Thin client protocol', 'Parser.OP_PUT', '1000'),
        BASE.replace(/CacheView/g, 'CachesView').replace('position 2, int', 'position 3, int')].join('\n');

    assert.deepStrictEqual(check.compare(base, merge).map(f => [f.key, f.before, f.after]), [
        ['system view CachesView column cacheId', null, 'position 3, int'],
        ['system view CachesView column cacheName', null, 'position 1, java.lang.String'],
        ['system view CacheView column cacheId', 'position 2, int', null],
        ['system view CacheView column cacheName', 'position 1, java.lang.String', null],
        ['Parser.OP_GET', '1000', '1001'],
        ['Parser.OP_PUT', '1001', '1000'],
    ]);
});

test('a removed operation and a new error code with the same value are both reported', () => {
    const base = constant('Thin client protocol', 'ClientMessageParser.OP_CACHE_GET_ALL', '1003');
    const merge = constant('Thin client protocol', 'ClientStatus.CACHE_LOCKED', '1003');

    assert.deepStrictEqual(check.compare(base, merge).map(f => [f.key, f.before, f.after]), [
        ['ClientMessageParser.OP_CACHE_GET_ALL', '1003', null],
        ['ClientStatus.CACHE_LOCKED', null, '1003'],
    ]);
});

test('a constant renamed in one enum is a rename when another enum has a constant with the old name', () => {
    const server = constant('Thin client protocol', 'ClientBitmaskFeature.EXECUTE_TASK_BY_NAME', '1');
    const client = constant('Thin client protocol', 'ProtocolBitmaskFeature.EXECUTE_TASK_BY_NAME', '1');
    const renamed = constant('Thin client protocol', 'ClientBitmaskFeature.EXECUTE_TASK', '1');

    assert.deepStrictEqual(check.compare([server, client].join('\n'), [renamed, client].join('\n')), []);
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

test('a changed deprecated parameter points at its declaration, a removed one at the deprecation of its command', () => {
    const decl = value => line('CLI', 'control.sh --deactivate --force', value, 'deprecated', 'DeactivateCommandArg.java:33', '',
        'DeactivateCommand.java:33');
    const location = merge => check.compare(decl('optional, boolean'), merge).map(f => [f.kind, f.location]);

    assert.deepStrictEqual(location(decl('required, boolean')), [['contract', 'DeactivateCommandArg.java:33']]);
    assert.deepStrictEqual(location(''), [['deprecated-removal', 'DeactivateCommand.java:33']]);
});

test('a link counts only if its file is on the target branch and the link leads to it', async () => {
    const body = 'Covered by https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-existing.md. ' +
        'See [RFC](https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-9-typo.md#compatibility), ' +
        'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-10-permalink.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-absent.md, ' +
        'https://github.com/apache/ignite/blob/master/rfcs/Bad_Name.md and https://github.com/apache/ignite/pull/13760.';
    const onTarget = f => ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-9-typo.md', 'rfcs/IGNITE-10-permalink.md'].includes(f);
    const checked = [];
    const atRef = async (ref, f) => checked.push(`${ref}:${f}`) && ref === '1a2b3c';

    assert.deepStrictEqual(await check.rfcLinks(body, 'apache/ignite', 'master', onTarget, atRef), {
        linked: ['rfcs/IGNITE-7-existing.md', 'rfcs/IGNITE-10-permalink.md'],
        badLinks: [
            { path: 'rfcs/IGNITE-9-typo.md', reason: 'url' },
            { path: 'rfcs/IGNITE-8-absent.md', reason: 'missing' },
            { path: 'rfcs/Bad_Name.md', reason: 'name' },
        ],
    });
    assert.deepStrictEqual(checked, ['mastr:rfcs/IGNITE-9-typo.md', '1a2b3c:rfcs/IGNITE-10-permalink.md']);
});

test('a broken link does not hide a working link to the same file, and a path past the file is not a link to it', async () => {
    const body = 'https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-7-y.md and then ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md, again ' +
        'https://github.com/apache/ignite/tree/master/rfcs/IGNITE-7-y.md, also ' +
        'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-8-z.md/extra';
    const onTarget = f => ['rfcs/IGNITE-7-y.md', 'rfcs/IGNITE-8-z.md'].includes(f);

    assert.deepStrictEqual(await check.rfcLinks(body, 'apache/ignite', 'master', onTarget, async () => false), {
        linked: ['rfcs/IGNITE-7-y.md'],
        badLinks: [{ path: 'rfcs/IGNITE-8-z.md/extra', reason: 'name' }],
    });
});

test('only links to the target repository count', async () => {
    const body = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md ' +
        'https://github.com/owner/standXrepo/blob/master/rfcs/IGNITE-8-z.md ' +
        'https://github.com/owner/stand.repo/blob/master/rfcs/IGNITE-9-w.md';

    assert.deepStrictEqual(await check.rfcLinks(body, 'owner/stand.repo', 'master', () => true, async () => false),
        { linked: ['rfcs/IGNITE-9-w.md'], badLinks: [] });
});

test('a branch name may contain slashes', async () => {
    const body = 'https://github.com/apache/ignite/blob/release/docs/rfcs/IGNITE-7-y.md';
    const checked = [];
    const atRef = async (ref, f) => checked.push(`${ref}:${f}`) > 0;

    assert.deepStrictEqual(await check.rfcLinks(body, 'apache/ignite', 'master', () => true, atRef),
        { linked: ['rfcs/IGNITE-7-y.md'], badLinks: [] });
    assert.deepStrictEqual(checked, ['release/docs:rfcs/IGNITE-7-y.md']);
});

test('one confirmed RFC is enough when GitHub cannot check another link, in either order of the links', async () => {
    const onTarget = () => true;
    const atRef = async () => { throw Object.assign(new Error('Server Error'), { status: 502 }); };
    const local = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-7-y.md';

    for (const other of ['rfcs/IGNITE-7-y.md', 'rfcs/IGNITE-8-z.md']) {
        const remote = `https://github.com/apache/ignite/blob/1a2b3c/${other}`;

        for (const body of [`${local} ${remote}`, `${remote} ${local}`]) {
            assert.deepStrictEqual(await check.rfcLinks(body, 'apache/ignite', 'master', onTarget, atRef),
                { linked: ['rfcs/IGNITE-7-y.md'], badLinks: [] }, body);
        }
    }

    const unchecked = 'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-8-z.md';

    await assert.rejects(check.rfcLinks(unchecked, 'apache/ignite', 'master', onTarget, atRef), /Server Error/);
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
    assert.throws(() => check.validate({ ...ok, outcome: 'rfc-found', rfcs: [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr', blob: 'not a blob' }] }));
    assert.throws(() => check.validate({ ...ok, outcome: 'review' }));
    assert.throws(() => check.validate({ ...ok, outcome: 'rfc-found' }));
    assert.throws(() => check.validate({ ...ok, notShown: 0 }));
    assert.throws(() => check.validate({ ...ok, notShown: '1' }));
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

test('a linked RFC is shown on the target repository and branch', () => {
    const rfcs = [{ path: 'rfcs/IGNITE-7-y.md', source: 'link' }];
    const res = { outcome: 'rfc-found', findings: [CONTRACT], rfcs, invalidNames: [], badLinks: [] };
    const md = check.render(res, { baseRepoUrl: 'https://github.com/owner/stand', baseRef: 'release/docs' }, 10);

    assert.ok(md.includes('(https://github.com/owner/stand/blob/release/docs/rfcs/IGNITE-7-y.md)'), md);
});

test('long reports are cut to stay within the GitHub comment limit', () => {
    const findings = Array.from({ length: 30 }, (_, i) => ({ ...CONTRACT, key: 'k' + i, after: 'x'.repeat(i === 0 ? 1000 : 1) }));
    const md = check.render({ outcome: 'rfc-missing', findings, rfcs: [], invalidNames: [], badLinks: [] }, {}, 10);

    assert.strictEqual(md.split('\n').find(l => l.startsWith('+ k0')).length, 500);
    assert.ok(md.includes('+ k9: x') && !md.includes('+ k10: x'), md);
    assert.ok(md.includes('20 more changes are not shown here.'), md);
});

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
async function publish(gh, res) {
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

    await check.publish({ github: gh, context, core: { info() {}, warning() {} }, resultFile: file, target: 'master' });
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

test('a new version of the RFC text replaces the comment, so its link leads to the current text', async () => {
    const gh = github();
    const found = check.decide([CONTRACT], { ...NO_RFC, files: ['rfcs/IGNITE-1-x.md'] });
    const withBlob = blob => ({ ...found, rfcs: [{ path: 'rfcs/IGNITE-1-x.md', source: 'pr', blob }] });

    await publish(gh, withBlob('a'.repeat(40)));
    await publish(gh, withBlob('a'.repeat(40)));
    await publish(gh, withBlob('b'.repeat(40)));

    assert.deepStrictEqual(gh.calls, ['create', 'delete 100', 'create']);
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

test('a result is ignored when a newer run checks the same commit of the same PR', async () => {
    const gh = github({ runs: [{ number: 1, branch: 'ignite-1' }, { number: 2, branch: 'ignite-1' }] });

    await publish(gh, check.decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, []);
});

test('a newer run of another PR with the same commit does not suppress the result', async () => {
    const gh = github({ runs: [{ number: 1, branch: 'ignite-1' }, { number: 2, branch: 'ignite-1-copy' }] });

    await publish(gh, check.decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.calls, ['create']);
});

test('the comment goes to the PR into the target branch, not to a PR of the same branch into a release branch', async () => {
    const gh = github({ prs: [{ number: 8, base: 'ignite-2.17' }, { number: 7, base: 'master' }] });

    await publish(gh, check.decide([CONTRACT], NO_RFC));

    assert.deepStrictEqual(gh.commented, [7]);
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

    for (const [f, text] of Object.entries(onTarget))
        fs.writeFileSync(path.join(dir, f), text);

    git(dir, 'add', '.');
    git(dir, 'commit', '-q', '--allow-empty', '-m', 'target');

    for (const [f, text] of Object.entries(files))
        fs.writeFileSync(path.join(dir, f), text);

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

const BUILT = { checkout: 'success', java: 'success', base: 'success', merge: 'success' };
const WITH_CONTRACT_CHANGE = BASE + '\n' + line('CLI', 'control.sh --cache list --seq', 'optional, boolean');

/**
 * Runs report() in the workspace.
 *
 * @param body PR description.
 * @param getContent Stub of the GitHub API call that reads a file at a ref.
 * @param repo Target repository of the PR.
 */
async function report(ws, {
    steps = BUILT, body = '', getContent = async () => { throw new Error('unexpected'); }, repo = APACHE,
} = {}) {
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

    await check.report({ github: { rest: { repos: { getContent } } }, core, context, steps, workspace: ws });

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

test('report: a link with a mistyped branch does not replace a missing RFC, and GitHub is asked about the target repository', async () => {
    const ws = workspace({ onTarget: { 'rfcs/IGNITE-7-y.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE });
    const stand = { owner: { login: 'owner' }, name: 'stand', full_name: 'owner/stand', html_url: 'https://github.com/owner/stand' };
    const asked = [];
    const getContent = async args => {
        asked.push(args);

        throw Object.assign(new Error('Not Found'), { status: 404 });
    };
    const out = await report(ws, { body: 'https://github.com/owner/stand/blob/mastr/rfcs/IGNITE-7-y.md', getContent, repo: stand });

    assert.deepStrictEqual([out.result.outcome, out.result.badLinks], ['rfc-missing', [{ path: 'rfcs/IGNITE-7-y.md', reason: 'url' }]]);
    assert.deepStrictEqual(asked, [{ owner: 'owner', repo: 'stand', path: 'rfcs/IGNITE-7-y.md', ref: 'mastr' }]);
});

test('report: a link that GitHub cannot confirm fails as a check that could not finish', async () => {
    const ws = workspace({ onTarget: { 'rfcs/IGNITE-7-y.md': 'rfc' }, merge: WITH_CONTRACT_CHANGE });
    const getContent = async () => { throw Object.assign(new Error('Server Error'), { status: 502 }); };
    const out = await report(ws, { body: 'https://github.com/apache/ignite/blob/1a2b3c/rfcs/IGNITE-7-y.md', getContent });

    assert.deepStrictEqual(out.result, { outcome: 'error', step: 'links' });
});

test('report: links are not checked when the PR needs no RFC', async () => {
    const merge = BASE.replace(line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated') + '\n', '');
    const ws = workspace({ onTarget: { 'rfcs/IGNITE-7-y.md': 'rfc' }, merge });
    const out = await report(ws, { body: 'https://github.com/apache/ignite/blob/mastr/rfcs/IGNITE-7-y.md' });

    assert.strictEqual(out.result.outcome, 'review');
});

test('report: a PR without source changes passes quietly', async () => {
    const out = await report(workspace({ files: { 'rfcs/IGNITE-1-x.md': 'rfc' } }),
        { steps: { checkout: 'success', java: 'skipped', base: 'skipped', merge: 'skipped' } });

    assert.deepStrictEqual([out.result, out.annotations], [{ outcome: 'clean' }, []]);
});

test('report: a failed Java setup fails as a check that could not finish, not as a clean PR', async () => {
    const out = await report(workspace(), { steps: { checkout: 'success', java: 'failure', base: 'skipped', merge: 'skipped' } });

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
