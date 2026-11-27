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
// the RFC and shows the report in the job summary.

'use strict';

const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');
const { compare } = require('./compare.js');
const { rfcFiles, rfcLinks } = require('./rfc.js');
const { FAILED, decide } = require('./outcome.js');
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

/** @return Whether the PR changes the files of the check and nothing else. */
function changesOnlyCheck(git) {
    const files = git('merge', 'diff', '--no-renames', '--name-only', '-z', 'HEAD^1', 'HEAD').split('\0').filter(Boolean);

    return files.length > 0 && files.every(f => CHECK_FILES.some(prefix => f.startsWith(prefix)));
}

/** @return RFC files of the PR and RFC links in its description. */
function findRfc({ pr, git, workspace }) {
    const onTarget = f => fs.statSync(path.join(workspace, 'base', f), { throwIfNoEntry: false })?.isFile() === true;

    return {
        ...rfcFiles(git('merge', 'diff', '--no-renames', '--name-only', '--diff-filter=d', '-z', 'HEAD^1', 'HEAD', '--', 'rfcs/')),
        ...rfcLinks(pr.body, pr.base.repo.full_name, onTarget),
    };
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

            res = decide(findings, findRfc({ pr, git, workspace }), checkOnly);
        }
    }
    catch (e) {
        core.error(e.stack || String(e));

        res = { outcome: 'error', step: 'report' };
    }

    const links = { headRepoUrl: pr.head.repo.html_url, headSha: pr.head.sha, baseRepoUrl: pr.base.repo.html_url, baseRef: pr.base.ref };

    await core.summary.addRaw(render(res, links, SUMMARY_LINES)).write();

    if (FAILED.includes(res.outcome))
        core.setFailed(`${title(res)}. See the job summary.`);
    else if (res.outcome !== 'clean')
        core.warning(`${title(res)}. See the job summary for the detected changes.`, { title: title(res) });
}

module.exports = { report };
