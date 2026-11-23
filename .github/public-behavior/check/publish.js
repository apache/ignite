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

// Runs in the workflow_run job, which runs the code of the target branch and can write to the PR. It treats result.json of
// the PR job as untrusted data and renders the comment itself.

'use strict';

const crypto = require('crypto');
const fs = require('fs');
const { validate } = require('./outcome.js');
const { render } = require('./render.js');

const MARKER = '<!-- ignite-public-behavior-changes';
const COMMENT_LINES = 100;

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
 * Replaces the bot comment when the result changes. A newer run for the same commit of the same PR branch, started for example
 * by an edit of the PR description, publishes instead of this one. A run of another PR with the same commit does not count.
 *
 * @param resultFile result.json of the PR job.
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

module.exports = { publish };
