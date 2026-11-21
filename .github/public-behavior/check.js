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

// The "Public behavior changes" check. report() runs in the PR job, which builds PR code and has a read-only token.
// publish() runs in the workflow_run job, which can write to the PR: it treats result.json as untrusted data
// and renders the comment itself.

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

const MARKER = '<!-- ignite-public-behavior-changes';
const RFC_NAME = /^rfcs\/IGNITE-\d+-[a-z0-9]+(?:-[a-z0-9]+)*\.md$/;
const RFC_LINK = /https:\/\/github\.com\/apache\/ignite\/(?:blob|tree)\/[^/\s]+\/(rfcs\/[A-Za-z0-9._-]*[A-Za-z0-9_-])/g;
const TEMPLATE = 'rfcs/TEMPLATE.md';
const RFC_EXAMPLE = 'rfcs/IGNITE-12345-cache-timeout.md';
const PROCESS_URL = 'https://github.com/apache/ignite/tree/master/rfcs';
const CONTRACT_URL = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-29219-public-api-and-compatibility.md';
const KINDS = ['contract', 'deprecated-removal', 'experimental'];
const OUTCOMES = ['clean', 'review', 'rfc-found', 'rfc-name', 'rfc-missing', 'error'];
const FAILED = ['rfc-name', 'rfc-missing', 'error'];
const STEPS = ['checkout', 'base', 'merge', 'report', 'result'];
const COMMENT_LINES = 100;
const SUMMARY_LINES = 2000;
const LINE_CHARS = 500;

/** Parses the output of PublicDeclarations.java. */
function parse(text) {
    const res = new Map();

    for (const line of text.split('\n')) {
        if (!line)
            continue;

        const [area, key, value = '', flags = '', location = ''] = line.split('\t');

        res.set(area + '\t' + key, { area, key, value, flags: flags.split(',').filter(Boolean), location });
    }

    return res;
}

/**
 * @return Declarations added, removed or changed by the PR. A removed deprecated declaration and any change to an experimental
 *      declaration have their own kinds: the reviewer decides whether they need an RFC.
 */
function compare(baseText, mergeText) {
    const base = parse(baseText);
    const merge = parse(mergeText);
    const res = [];

    const finding = (b, m) => {
        const kind = !m && b.flags.includes('deprecated') ? 'deprecated-removal'
            : (b || m).flags.includes('experimental') ? 'experimental' : 'contract';

        res.push({
            area: (b || m).area,
            key: (b || m).key,
            before: b ? b.value : null,
            after: m ? m.value : null,
            kind,
            location: m ? m.location : b.location,
        });
    };

    for (const [id, b] of base) {
        const m = merge.get(id);

        if (!m || m.value !== b.value)
            finding(b, m);
    }

    for (const [id, m] of merge) {
        if (!base.has(id))
            finding(null, m);
    }

    return res.sort((a, b) => a.area.localeCompare(b.area) || a.key.localeCompare(b.key));
}

/**
 * @param nameStatus Output of `git diff --name-status -M` between the target branch and the PR merge for rfcs/.
 * @param body PR description.
 * @param existsOnTarget Checks that a file exists on the target branch.
 * @return RFC files that the PR adds or updates, their invalid names, and RFC files linked from the description.
 */
function findRfcs(nameStatus, body, existsOnTarget) {
    const files = [];

    for (const line of nameStatus.split('\n')) {
        const [status, ...paths] = line.split('\t');
        const file = paths[paths.length - 1];

        if (status && status !== 'D' && file.endsWith('.md') && file !== TEMPLATE)
            files.push(file);
    }

    const linked = [];
    const badLinks = [];

    for (const m of (body || '').matchAll(RFC_LINK)) {
        const file = m[1];

        if (linked.includes(file) || badLinks.some(l => l.path === file))
            continue;

        if (RFC_NAME.test(file) && existsOnTarget(file))
            linked.push(file);
        else
            badLinks.push({ path: file, reason: RFC_NAME.test(file) ? 'missing' : 'name' });
    }

    return { files, invalidNames: files.filter(f => !RFC_NAME.test(f)), linked, badLinks };
}

/** @return Outcome for described declarations, following the decision tree of IGNITE-29326. */
function decide(findings, rfcs) {
    if (findings.length === 0)
        return { outcome: 'clean' };

    const res = { findings, rfcs: [], invalidNames: [], badLinks: [] };

    if (findings.every(f => f.kind !== 'contract'))
        return { outcome: 'review', ...res };

    if (rfcs.files.length > 0) {
        return rfcs.invalidNames.length > 0
            ? { outcome: 'rfc-name', ...res, invalidNames: rfcs.invalidNames }
            : { outcome: 'rfc-found', ...res, rfcs: rfcs.files.map(p => ({ path: p, source: 'pr' })) };
    }

    return rfcs.linked.length > 0
        ? { outcome: 'rfc-found', ...res, rfcs: rfcs.linked.map(p => ({ path: p, source: 'link' })) }
        : { outcome: 'rfc-missing', ...res, badLinks: rfcs.badLinks };
}

/** Rejects anything that the PR job could not have produced. */
function validate(res) {
    const str = (s, max = 4000) => typeof s === 'string' && s.length <= max && !/[\r\n\t]/.test(s);
    const opt = s => s === null || str(s);
    const list = (a, max, ok) => Array.isArray(a) && a.length <= max && a.every(x => x && ok(x));
    const fail = what => { throw new Error('Invalid result: ' + what); };

    if (!res || !OUTCOMES.includes(res.outcome))
        fail('outcome');

    if (res.outcome === 'error')
        return { outcome: 'error', step: STEPS.includes(res.step) ? res.step : 'result' };

    if (res.outcome === 'clean')
        return { outcome: 'clean' };

    if (!list(res.findings, 100000, f => str(f.area, 100) && str(f.key) && opt(f.before) && opt(f.after)
        && (f.before !== null || f.after !== null) && KINDS.includes(f.kind) && str(f.location, 300)
        && (f.hint === undefined || str(f.hint, 300))) || res.findings.length === 0)
        fail('findings');

    const contract = res.findings.some(f => f.kind === 'contract');

    if (!list(res.rfcs, 20, r => RFC_NAME.test(r.path) && ['pr', 'link'].includes(r.source))
        || (res.outcome === 'rfc-found') !== (res.rfcs.length > 0) || (res.outcome === 'review') === contract)
        fail('rfcs');

    if (!list(res.invalidNames, 20, p => str(p, 300)) || (res.outcome === 'rfc-name') !== (res.invalidNames.length > 0))
        fail('invalidNames');

    if (!list(res.badLinks, 20, l => str(l.path, 300) && ['name', 'missing'].includes(l.reason)))
        fail('badLinks');

    return {
        outcome: res.outcome,
        findings: res.findings.map(({ area, key, before, after, kind, location, hint }) =>
            ({ area, key, before, after, kind, location, ...(hint === undefined ? {} : { hint }) })),
        rfcs: res.rfcs.map(({ path: p, source }) => ({ path: p, source })),
        invalidNames: [...res.invalidNames],
        badLinks: res.badLinks.map(({ path: p, reason }) => ({ path: p, reason })),
    };
}

/** @return Inline code that cannot break out of its backticks. */
function code(s) {
    return '`' + String(s).replace(/[`\r\n]/g, '') + '`';
}

/** @return Diff block whose fence is longer than any backtick run in the text. */
function diffBlock(text) {
    const longest = (text.match(/`+/g) || []).reduce((max, run) => Math.max(max, run.length), 2);
    const fence = '`'.repeat(longest + 1);

    return fence + 'diff\n' + text + '\n' + fence;
}

/** @return Diff line, cut to a length that keeps the comment within the GitHub limit. */
function diffLine(sign, f, val, last) {
    const note = { 'deprecated-removal': ` [${f.hint || 'deprecated'}]`, 'experimental': ' [experimental]' }[f.kind] || '';
    const line = sign + ' ' + f.key + (val ? ': ' + val : '') + (last ? note + (f.location ? ' (' + f.location + ')' : '') : '');

    return line.length > LINE_CHARS ? line.slice(0, LINE_CHARS - 3) + '...' : line;
}

/** @return Detected changes grouped by area, at most `limit` lines. */
function renderFindings(findings, limit, runUrl) {
    const out = [];
    let lines = 0;
    let shown = 0;

    for (const area of [...new Set(findings.map(f => f.area))]) {
        const items = findings.filter(f => f.area === area);
        const diff = [];

        for (const f of items) {
            const cost = f.before !== null && f.after !== null ? 2 : 1;

            if (lines + cost > limit)
                break;

            if (f.before !== null)
                diff.push(diffLine('-', f, f.before, f.after === null));

            if (f.after !== null)
                diff.push(diffLine('+', f, f.after, true));

            lines += cost;
            shown++;
        }

        if (diff.length === 0)
            break;

        out.push(`**${area.replace(/[^A-Za-z ]/g, '')}** (${items.length})`, '', diffBlock(diff.join('\n')), '');
    }

    if (shown < findings.length)
        out.push(`${findings.length - shown} more changes are not shown here.` + (runUrl ? ` See the [check summary](${runUrl}).` : ''));

    return out.join('\n');
}

/** @return Link to an RFC file. */
function rfcUrl(rfc, links) {
    return rfc.source === 'pr'
        ? `${links.headRepoUrl}/blob/${links.headSha}/${rfc.path}`
        : `https://github.com/apache/ignite/blob/${links.baseRef.split('/').map(encodeURIComponent).join('/')}/${rfc.path}`;
}

/** @return Title of an outcome. */
function title(res) {
    return {
        'clean': 'No public behavior changes',
        'review': 'Review deprecated or experimental API changes',
        'rfc-found': 'Review the RFC',
        'rfc-name': 'Fix the RFC filename',
        'rfc-missing': 'RFC needed',
        'error': 'Check could not finish',
    }[res.outcome];
}

/**
 * @param res Validated result.
 * @param links Head repository URL, head SHA, base ref and, in the comment, the run URL.
 * @param limit Maximum number of change lines.
 * @return Markdown report.
 */
function render(res, links, limit) {
    const out = [`### Public behavior changes: ${title(res)}`, ''];

    if (res.outcome === 'clean') {
        out.push('The check found no change to the public contract.');

        return out.join('\n');
    }

    if (res.outcome === 'error') {
        out.push({
            'base': 'The check could not build the target branch or describe its public declarations. Rerun the check.',
            'merge': 'The check could not build the PR merged into the target branch or describe its public declarations. ' +
                'If the PR does not compile, fix it. Otherwise rerun the check.',
        }[res.step] || 'The check failed for a technical reason. Rerun the check.');

        if (links.runUrl)
            out.push('', `The [check log](${links.runUrl}) shows the cause.`);

        return out.join('\n');
    }

    if (res.outcome === 'review') {
        out.push('This PR removes deprecated declarations or changes experimental API. ' +
            `The [RFC process](${PROCESS_URL}) lets authors remove APIs deprecated before it without an RFC, ` +
            'and experimental APIs may change in any release.',
            '',
            'Reviewer: decide whether these changes need an RFC. For a deprecated declaration, check when it was deprecated.');
    }
    else if (res.outcome === 'rfc-found') {
        out.push(`RFC: ${res.rfcs.map(r => `[${r.path}](${rfcUrl(r, links)})`).join(', ')}.`,
            '',
            'Reviewer: check that the RFC covers the changes below, review compatibility and verify any required vote on the dev list.');
    }
    else if (res.outcome === 'rfc-name') {
        out.push(...res.invalidNames.map(p => `- ${code(p)} does not match ${code('rfcs/IGNITE-<ticket>-<short-title>.md')}.`),
            '',
            `Rename the file, for example to ${code(RFC_EXAMPLE)}.`);
    }
    else {
        out.push(`This PR changes the [public contract](${CONTRACT_URL}). ` +
            `The [RFC process](${PROCESS_URL}) requires an RFC for such changes, including compatible additions.`,
            '',
            `Add or update an RFC file in this PR, for example ${code(RFC_EXAMPLE)}. ` +
            'Or link an existing RFC file on the target branch from the PR description.');

        if (res.badLinks.length > 0) {
            out.push('', ...res.badLinks.map(l => `- ${code(l.path)} ` +
                (l.reason === 'missing' ? 'is not on the target branch.' : `does not match ${code('rfcs/IGNITE-<ticket>-<short-title>.md')}.`)));
        }
    }

    out.push('', 'Detected changes (`-` target branch, `+` this PR):', '', renderFindings(res.findings, limit, links.runUrl));

    return out.join('\n').trimEnd();
}

/** @return Name of the first step that did not succeed, or undefined. */
function failedStep(outcomes) {
    return Object.keys(outcomes).find(s => outcomes[s] !== 'success' && outcomes[s] !== 'skipped');
}

/**
 * Adds to each removed deprecated declaration when its deprecation was last changed: the commit and the first release that
 * contains it. Fetches the full history when the checkout is shallow. A repository without release tags, such as a fork,
 * gives no release. A hint that cannot be computed is left out.
 */
function addDeprecationHints(findings, repo, core) {
    const removed = findings.filter(f => f.kind === 'deprecated-removal' && /:\d+$/.test(f.location));

    if (removed.length === 0)
        return;

    const git = (...args) => execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8', maxBuffer: 64 << 20 }).trim();
    const releaseTags = tags => tags.split('\n').filter(t => /^\d+\.\d+\.\d+$/.test(t));

    try {
        if (git('rev-parse', '--is-shallow-repository') === 'true')
            git('fetch', '--quiet', '--unshallow', '--tags', 'origin');

        const released = releaseTags(git('tag', '-l')).length > 0;

        for (const f of removed) {
            const [file, line] = [f.location.replace(/:\d+$/, ''), f.location.match(/(\d+)$/)[1]];
            const sha = git('blame', '--porcelain', '-L', `${line},${line}`, 'HEAD', '--', file).split(' ')[0];
            const date = git('log', '-1', '--format=%as', sha);
            const release = releaseTags(git('tag', '--contains', sha, '--sort=creatordate'))[0];

            f.hint = `deprecated in ${sha.slice(0, 10)} on ${date}`
                + (release ? `, first release ${release}` : released ? ', not released yet' : '');
        }
    }
    catch (e) {
        core.warning(`Could not find when the removed declarations were deprecated: ${e.message}`);
    }
}

/** Runs in the PR job after both versions are described. */
async function report({ core, context, steps, workspace = '.' }) {
    const pr = context.payload.pull_request;
    const read = f => fs.existsSync(path.join(workspace, f)) ? fs.readFileSync(path.join(workspace, f), 'utf8') : '';

    let res = { outcome: 'error', step: failedStep(steps) };

    try {
        if (!res.step) {
            const nameStatus = execFileSync('git', ['-C', path.join(workspace, 'merge'), 'diff', '--name-status', '-M', 'HEAD^1', 'HEAD',
                '--', 'rfcs/'], { encoding: 'utf8' });
            const onTarget = f => fs.statSync(path.join(workspace, 'base', f), { throwIfNoEntry: false })?.isFile() === true;
            const findings = compare(read('base.txt'), read('merge.txt'));

            addDeprecationHints(findings, path.join(workspace, 'base'), core);

            res = validate(decide(findings, findRfcs(nameStatus, pr.body, onTarget)));
        }
    }
    catch (e) {
        core.error(e.stack || String(e));

        res = { outcome: 'error', step: 'report' };
    }

    fs.writeFileSync(path.join(workspace, 'result.json'), JSON.stringify(res));

    const links = { headRepoUrl: pr.head.repo.html_url, headSha: pr.head.sha, baseRef: pr.base.ref };

    await core.summary.addRaw(render(res, links, SUMMARY_LINES)).write();

    if (FAILED.includes(res.outcome))
        core.setFailed(`${title(res)}. See the job summary.`);
    else if (res.outcome !== 'clean')
        core.warning(`${title(res)}. See the job summary for the detected changes.`, { title: title(res) });
}

/** Finds the open PR whose head is the commit that the run checked. */
async function findPr(github, owner, repo, run) {
    if (run.pull_requests?.length > 0) {
        const { data } = await github.rest.pulls.get({ owner, repo, pull_number: run.pull_requests[0].number });

        return data.head.sha === run.head_sha ? data : null;
    }

    const prs = await github.paginate(github.rest.pulls.list, {
        owner, repo, state: 'open', head: `${run.head_repository.owner.login}:${run.head_branch}`, per_page: 100,
    });

    return prs.find(p => p.head.sha === run.head_sha) || null;
}

/** Runs in the workflow_run job: replaces the bot comment when the result changes. */
async function publish({ github, context, core, resultFile }) {
    const { owner, repo } = context.repo;
    const run = context.payload.workflow_run;
    const pr = await findPr(github, owner, repo, run);

    if (!pr) {
        core.info(`No open PR has head ${run.head_sha}; the result is outdated.`);

        return;
    }

    let res;

    try {
        res = validate(JSON.parse(fs.readFileSync(resultFile, 'utf8')));
    }
    catch (e) {
        core.warning(`Treating the run as a technical failure: ${e.message}`);

        res = { outcome: 'error', step: 'result' };
    }

    const links = { headRepoUrl: run.head_repository.html_url, headSha: run.head_sha, baseRef: pr.base.ref, runUrl: run.html_url };
    const state = `${MARKER} state:${crypto.createHash('sha256').update(JSON.stringify(res)).digest('hex').slice(0, 16)} -->`;

    const comments = await github.paginate(github.rest.issues.listComments, { owner, repo, issue_number: pr.number, per_page: 100 });
    const own = comments.filter(c => c.user?.login === 'github-actions[bot]' && c.body?.startsWith(MARKER));

    if (own.length === 0 && res.outcome === 'clean')
        return;

    if (own.length === 1 && own[0].body.startsWith(state))
        return;

    for (const c of own)
        await github.rest.issues.deleteComment({ owner, repo, comment_id: c.id });

    const resolved = res.outcome === 'clean' ? ' All earlier issues are resolved.' : '';

    await github.rest.issues.createComment({
        owner, repo, issue_number: pr.number, body: state + '\n' + render(res, links, COMMENT_LINES) + resolved,
    });
}

module.exports = { compare, findRfcs, decide, validate, render, report, publish };
