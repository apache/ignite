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

// Runs in the PR job, which builds PR code and has a read-only token: compares the descriptions of both builds, looks for
// the RFC, writes result.json for the job that comments on the PR and shows the report in the job summary.

'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { compare } = require('./compare.js');
const { NO_RFC, rfcFiles, rfcLinks } = require('./rfc.js');
const { FAILED, decide, shorten, validate } = require('./outcome.js');
const { title, render } = require('./render.js');

/** Files of the check. A PR that changes nothing else changes only how the check describes declarations. */
const CHECK_FILES = ['.github/public-behavior/', '.github/workflows/public-behavior-changes'];
const SUMMARY_LINES = 2000;

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

/** @return Whether the PR changes the files of the check and nothing else. */
function changesOnlyCheck(git) {
    const files = git('merge', 'diff', '--no-renames', '--name-only', '-z', 'HEAD^1', 'HEAD').split('\0').filter(Boolean);

    return files.length > 0 && files.every(f => CHECK_FILES.some(prefix => f.startsWith(prefix)));
}

/**
 * Looks for an RFC only when the PR changes the public contract: first among the files of the PR, then among the links in
 * its description. Each RFC gets the blob of its file, so the comment changes when the RFC text changes.
 */
function findRfc(findings, { pr, git, workspace }) {
    if (!findings.some(f => f.kind === 'contract'))
        return NO_RFC;

    const rfcs = rfcFiles(git('merge', 'diff', '--no-renames', '--name-only', '--diff-filter=d', '-z', 'HEAD^1', 'HEAD', '--', 'rfcs/'));

    if (rfcs.files.length === 0) {
        const onTarget = f => fs.statSync(path.join(workspace, 'base', f), { throwIfNoEntry: false })?.isFile() === true;

        Object.assign(rfcs, rfcLinks(pr.body, pr.base.repo.full_name, onTarget));
    }

    rfcs.blobs = new Map([...rfcs.files.map(f => [f, git('merge', 'rev-parse', `HEAD:${f}`).trim()]),
        ...rfcs.linked.map(f => [f, git('base', 'rev-parse', `HEAD:${f}`).trim()])]);

    return rfcs;
}

/**
 * Runs after both builds are described.
 *
 * @param steps Outcomes of the workflow steps, in their order.
 * @param workspace Directory with the merge and base checkouts and their descriptions.
 */
async function report({ core, context, steps, workspace = '.' }) {
    const pr = context.payload.pull_request;
    const read = f => fs.existsSync(path.join(workspace, f)) ? fs.readFileSync(path.join(workspace, f), 'utf8') : '';
    const git = (dir, ...args) => execFileSync('git', ['-C', path.join(workspace, dir), ...args], { encoding: 'utf8' });

    let res = { outcome: 'error', step: failedStep(steps) };

    try {
        if (!res.step) {
            const findings = compare(read('base.txt'), read('merge.txt'));
            const checkOnly = findings.length > 0 && changesOnlyCheck(git);

            addDeprecationHints(findings, path.join(workspace, 'base'), core);

            const rfcs = checkOnly ? NO_RFC : findRfc(findings, { pr, git, workspace });
            const decided = decide(findings, rfcs, checkOnly);

            decided.rfcs = (decided.rfcs || []).map(r => ({ ...r, blob: rfcs.blobs.get(r.path) }));

            res = validate(shorten(decided));
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

module.exports = { report };
