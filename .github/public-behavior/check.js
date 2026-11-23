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
const RFC_PATH = /^(.+?)\/(rfcs\/.+)$/;
const BLOB = /^[0-9a-f]{40,64}$/;
const TEMPLATE = 'rfcs/TEMPLATE.md';
const RFC_EXAMPLE = 'rfcs/IGNITE-12345-cache-timeout.md';
const PROCESS_URL = 'https://github.com/apache/ignite/tree/master/rfcs';
const CONTRACT_URL = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-29219-public-api-and-compatibility.md';
const KINDS = ['contract', 'deprecated-removal', 'experimental'];
const OUTCOMES = ['clean', 'review', 'rfc-found', 'rfc-name', 'rfc-missing', 'error'];
const FAILED = ['rfc-name', 'rfc-missing', 'error'];
const STEPS = ['checkout', 'java', 'base', 'merge', 'links', 'report', 'result'];
const LINK_PROBLEMS = ['name', 'missing', 'url'];
const NO_RFC = { files: [], invalidNames: [], linked: [], badLinks: [] };
const COMMENT_LINES = 100;
const SUMMARY_LINES = 2000;
const LINE_CHARS = 500;

/** Parses the output of PublicDeclarations.java. */
function parse(text) {
    const res = new Map();

    for (const line of text.split('\n')) {
        if (!line)
            continue;

        const [area, key, value = '', flags = '', location = '', group = '', deprecation = ''] = line.split('\t');

        res.set(area + '\t' + key, { area, key, value, flags: flags.split(',').filter(Boolean), location, group, deprecation });
    }

    return res;
}

/**
 * @return Keys of the declarations whose rename group only changed its name: the group left the target branch, another group
 *      came in the PR, and their declarations are the same apart from the group name. A group is identified by its name and
 *      the part of the key before the name, such as the class of a constant: two enums may have constants with the same name.
 */
function renamedKeys(base, merge) {
    const groupId = d => d.area + '\t' + d.key.slice(0, d.key.indexOf(d.group)) + '\t' + d.group;

    const groups = (decls, other) => {
        const present = new Set([...other.values()].filter(d => d.group).map(groupId));
        const res = new Map();

        for (const [id, d] of decls) {
            const gid = groupId(d);

            if (!d.group || present.has(gid))
                continue;

            if (!res.has(gid))
                res.set(gid, { ids: [], parts: [] });

            res.get(gid).ids.push(id);
            res.get(gid).parts.push(d.key.replace(d.group, '\u0001') + '\u0000' + d.value);
        }

        return [...res.values()].map(g => ({ ids: g.ids, sig: g.ids[0].split('\t')[0] + '\u0002' + g.parts.sort().join('\u0002') }));
    };

    const added = groups(merge, base);
    const res = new Set();

    for (const left of groups(base, merge)) {
        const i = added.findIndex(g => g.sig === left.sig);

        if (i >= 0) {
            [...left.ids, ...added[i].ids].forEach(id => res.add(id));
            added.splice(i, 1);
        }
    }

    return res;
}

/**
 * @return Declarations added, removed or changed by the PR. A removed deprecated declaration and any change to an experimental
 *      declaration have their own kinds: the reviewer decides whether they need an RFC. A removed deprecated declaration points
 *      at the annotation that deprecates it, which can be on its type or parent command; any other finding points at the
 *      declaration.
 */
function compare(baseText, mergeText) {
    const base = parse(baseText);
    const merge = parse(mergeText);
    const renamed = renamedKeys(base, merge);
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
            location: kind === 'deprecated-removal' ? b.deprecation : (m || b).location,
        });
    };

    for (const [id, b] of base) {
        const m = merge.get(id);

        if (!renamed.has(id) && (!m || m.value !== b.value))
            finding(b, m);
    }

    for (const [id, m] of merge) {
        if (!renamed.has(id) && !base.has(id))
            finding(null, m);
    }

    return res.sort((a, b) => a.area.localeCompare(b.area) || a.key.localeCompare(b.key));
}

/**
 * @param nameStatus Output of `git diff --name-status -z -M` between the target branch and the PR merge for rfcs/.
 * @return RFC files that the PR adds or updates and the ones with invalid names.
 */
function rfcFiles(nameStatus) {
    const parts = nameStatus.split('\0');
    const files = [];

    for (let i = 0; i < parts.length && parts[i];) {
        const status = parts[i++];
        const file = /^[RC]/.test(status) ? parts[(i += 2) - 1] : parts[i++];

        if (status !== 'D' && file.endsWith('.md') && file !== TEMPLATE)
            files.push(file);
    }

    return { ...NO_RFC, files, invalidNames: files.filter(f => !RFC_NAME.test(f)) };
}

/**
 * Finds direct links to RFC files in the PR description. A link counts if its whole path is an RFC file on the target branch
 * and the link itself leads to that file. A file counts if any of its links counts. If GitHub cannot tell whether a link leads
 * to its file, the result is unknown only when no file counts: one RFC is enough.
 *
 * @param body PR description.
 * @param repo Target repository, such as {@code apache/ignite}.
 * @param baseRef Name of the target branch.
 * @param onTarget Checks that a file exists on the target branch.
 * @param atRef Checks that a file exists at a branch, tag or commit of the target repository. Throws when GitHub cannot tell.
 */
async function rfcLinks(body, repo, baseRef, onTarget, atRef) {
    const prefix = `https://github.com/${repo}/`.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(prefix + '(?:blob|tree)/([^\\s<>"\'()\\[\\]]+)', 'g');
    const linked = [];
    const problems = new Map();
    let failure = null;

    for (const [, rest] of (body || '').matchAll(pattern)) {
        const m = RFC_PATH.exec(rest.replace(/[.,;:!]+$/, '').replace(/[?#].*$/, ''));

        if (!m || linked.includes(m[2]))
            continue;

        const [, ref, file] = m;
        let problem = !RFC_NAME.test(file) ? 'name' : !onTarget(file) ? 'missing' : null;

        if (!problem && ref !== baseRef) {
            try {
                problem = await atRef(ref, file) ? null : 'url';
            }
            catch (e) {
                failure = failure || e;

                continue;
            }
        }

        if (!problem)
            linked.push(file);
        else if (!problems.has(file))
            problems.set(file, problem);
    }

    if (failure && linked.length === 0)
        throw failure;

    const badLinks = [...problems].filter(([file]) => !linked.includes(file)).map(([file, reason]) => ({ path: file, reason }));

    return { linked, badLinks };
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

    if (!list(res.rfcs, 20, r => RFC_NAME.test(r.path) && ['pr', 'link'].includes(r.source) && (r.blob === undefined || BLOB.test(r.blob)))
        || (res.outcome === 'rfc-found') !== (res.rfcs.length > 0) || (res.outcome === 'review') === contract)
        fail('rfcs');

    if (!list(res.invalidNames, 20, p => str(p, 300)) || (res.outcome === 'rfc-name') !== (res.invalidNames.length > 0))
        fail('invalidNames');

    if (!list(res.badLinks, 20, l => str(l.path, 300) && LINK_PROBLEMS.includes(l.reason)))
        fail('badLinks');

    return {
        outcome: res.outcome,
        findings: res.findings.map(({ area, key, before, after, kind, location, hint }) =>
            ({ area, key, before, after, kind, location, ...(hint === undefined ? {} : { hint }) })),
        rfcs: res.rfcs.map(({ path: p, source, blob }) => ({ path: p, source, ...(blob === undefined ? {} : { blob }) })),
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
        : `${links.baseRepoUrl}/blob/${links.baseRef.split('/').map(encodeURIComponent).join('/')}/${rfc.path}`;
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
 * @param links Head repository URL, head SHA, base repository URL, base ref and, in the comment, the run URL.
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
            'java': 'The check could not set up Java. Rerun the check.',
            'base': 'The check could not build the target branch or describe its public declarations. Rerun the check.',
            'merge': 'The check could not build the PR merged into the target branch or describe its public declarations. ' +
                'If the PR does not compile, fix it. Otherwise rerun the check.',
            'links': 'GitHub could not confirm the RFC links in the PR description. Rerun the check.',
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
            out.push('', ...res.badLinks.map(l => `- ${code(l.path)} ` + {
                name: `does not match ${code('rfcs/IGNITE-<ticket>-<short-title>.md')}.`,
                missing: 'is not on the target branch.',
                url: 'is on the target branch, but the link does not lead to it.',
            }[l.reason]));
        }
    }

    out.push('', 'Detected changes (`-` target branch, `+` this PR):', '', renderFindings(res.findings, limit, links.runUrl));

    return out.join('\n').trimEnd();
}

/**
 * @param steps Outcomes of the workflow steps, in their order. Steps are skipped when the PR changes no sources or after an
 *      earlier step failed.
 * @return Name of the first step that failed, or undefined.
 */
function failedStep(steps) {
    return Object.keys(steps).find(s => steps[s] !== 'success' && steps[s] !== 'skipped');
}

/**
 * Adds to each removed deprecated declaration when its deprecation was last changed: the commit and the first release that
 * contains it. A shallow checkout fetches the full history and the release tags of apache/ignite, because a fork may have
 * old tags only. A repository without release tags gives no release. A hint that cannot be computed is left out.
 */
function addDeprecationHints(findings, repo, core) {
    const removed = findings.filter(f => f.kind === 'deprecated-removal' && /:\d+$/.test(f.location));

    if (removed.length === 0)
        return;

    const git = (...args) => execFileSync('git', ['-C', repo, ...args], { encoding: 'utf8', maxBuffer: 64 << 20 }).trim();
    const releaseTags = tags => tags.split('\n').filter(t => /^\d+\.\d+\.\d+$/.test(t));

    try {
        if (git('rev-parse', '--is-shallow-repository') === 'true') {
            git('fetch', '--quiet', '--unshallow', 'origin');
            git('fetch', '--quiet', '--force', '--tags', 'https://github.com/apache/ignite.git');
        }

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

/**
 * Looks for an RFC only when the PR changes the public contract: first among the files of the PR, then among the links in
 * its description. Each RFC gets the blob of its file, so the comment changes when the RFC text changes.
 */
async function findRfc(findings, { github, pr, workspace }) {
    if (!findings.some(f => f.kind === 'contract'))
        return NO_RFC;

    const git = (dir, ...args) => execFileSync('git', ['-C', path.join(workspace, dir), ...args], { encoding: 'utf8' });
    const rfcs = rfcFiles(git('merge', 'diff', '--name-status', '-z', '-M', 'HEAD^1', 'HEAD', '--', 'rfcs/'));

    if (rfcs.files.length === 0) {
        const onTarget = f => fs.statSync(path.join(workspace, 'base', f), { throwIfNoEntry: false })?.isFile() === true;
        const atRef = async (ref, file) => {
            try {
                await github.rest.repos.getContent({ owner: pr.base.repo.owner.login, repo: pr.base.repo.name, path: file, ref });

                return true;
            }
            catch (e) {
                if (e.status === 404)
                    return false;

                throw Object.assign(new Error(`Could not check ${file} at ${ref}: ${e.message}`), { step: 'links' });
            }
        };

        Object.assign(rfcs, await rfcLinks(pr.body, pr.base.repo.full_name, pr.base.ref, onTarget, atRef));
    }

    rfcs.blobs = new Map([...rfcs.files.map(f => [f, git('merge', 'rev-parse', `HEAD:${f}`).trim()]),
        ...rfcs.linked.map(f => [f, git('base', 'rev-parse', `HEAD:${f}`).trim()])]);

    return rfcs;
}

/** Runs in the PR job after both versions are described. */
async function report({ github, core, context, steps, workspace = '.' }) {
    const pr = context.payload.pull_request;
    const read = f => fs.existsSync(path.join(workspace, f)) ? fs.readFileSync(path.join(workspace, f), 'utf8') : '';

    let res = { outcome: 'error', step: failedStep(steps) };

    try {
        if (!res.step) {
            const findings = compare(read('base.txt'), read('merge.txt'));

            addDeprecationHints(findings, path.join(workspace, 'base'), core);

            const rfcs = await findRfc(findings, { github, pr, workspace });
            const decided = decide(findings, rfcs);

            decided.rfcs = (decided.rfcs || []).map(r => ({ ...r, blob: rfcs.blobs.get(r.path) }));

            res = validate(decided);
        }
    }
    catch (e) {
        core.error(e.stack || String(e));

        res = { outcome: 'error', step: e.step || 'report' };
    }

    fs.writeFileSync(path.join(workspace, 'result.json'), JSON.stringify(res));

    const links = { headRepoUrl: pr.head.repo.html_url, headSha: pr.head.sha, baseRepoUrl: pr.base.repo.html_url, baseRef: pr.base.ref };

    await core.summary.addRaw(render(res, links, SUMMARY_LINES)).write();

    if (FAILED.includes(res.outcome))
        core.setFailed(`${title(res)}. See the job summary.`);
    else if (res.outcome !== 'clean')
        core.warning(`${title(res)}. See the job summary for the detected changes.`, { title: title(res) });
}

/**
 * Finds the open PR into the target branch whose head is the commit that the run checked. The same branch may have another PR
 * into another branch, which the check does not run for.
 */
async function findPr(github, owner, repo, run, target) {
    const prs = await github.paginate(github.rest.pulls.list, {
        owner, repo, state: 'open', head: `${run.head_repository.owner.login}:${run.head_branch}`, base: target, per_page: 100,
    });

    return prs.find(p => p.head.sha === run.head_sha) || null;
}

/**
 * Runs in the workflow_run job: replaces the bot comment when the result changes. A newer run for the same commit of the same
 * PR branch, started for example by an edit of the PR description, publishes instead of this one. A run of another PR with the
 * same commit does not count.
 *
 * @param target Branch whose PRs the check runs for.
 */
async function publish({ github, context, core, resultFile, target }) {
    const { owner, repo } = context.repo;
    const run = context.payload.workflow_run;
    const pr = await findPr(github, owner, repo, run, target);

    if (!pr) {
        core.info(`No open PR into ${target} has head ${run.head_sha}; the result is outdated.`);

        return;
    }

    const { data: runs } = await github.rest.actions.listWorkflowRuns({
        owner, repo, workflow_id: run.workflow_id, head_sha: run.head_sha, event: 'pull_request', per_page: 100,
    });
    const samePr = r => r.head_branch === run.head_branch && r.head_repository?.full_name === run.head_repository.full_name;

    if (runs.workflow_runs.some(r => samePr(r) && r.run_number > run.run_number)) {
        core.info(`A newer run checks ${run.head_sha}; it publishes the result.`);

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

    const links = {
        headRepoUrl: run.head_repository.html_url, headSha: run.head_sha, baseRepoUrl: pr.base.repo.html_url, baseRef: pr.base.ref,
        runUrl: run.html_url,
    };
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

module.exports = { compare, rfcFiles, rfcLinks, decide, validate, render, report, publish };
