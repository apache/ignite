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

const marker = '<!-- ignite-rolling-upgrade-check -->';
const label = 'compatibility';
const bot = 'github-actions[bot]';

function only(items) {
    return items.length === 1 ? items[0] : undefined;
}

function stepResult(job, name) {
    return only(job?.steps?.filter(step => step.name === name && step.status === 'completed') ?? [])?.conclusion;
}

module.exports = async function reportWireChanges({github, context, core}) {
    const run = context.payload.workflow_run;
    const repo = context.repo;
    const {actions, issues, pulls} = github.rest;
    const jobs = await github.paginate(actions.listJobsForWorkflowRun, {
        ...repo, run_id: run.id, filter: 'latest', per_page: 100,
    });

    if (jobs.some(job => job.run_attempt > run.run_attempt))
        return core.info('A newer attempt has started.');

    const headJobs = jobs.filter(job => job.head_sha === run.head_sha);
    const wire = only(headJobs.filter(job => job.name === 'Compare wire format descriptions'));
    const java = only(headJobs.filter(job => job.name === 'Check java code on JDK 17'));
    const upload = stepResult(wire, 'Upload wire diff');

    if (wire?.conclusion !== 'success' || stepResult(wire, 'Compare descriptions and publish summary') !== 'success'
        || stepResult(java, 'Check all wire format descriptions are collected') !== 'success'
        || !['success', 'skipped'].includes(upload))
        return core.info('The wire comparison or completeness check could not be verified.');

    // Fork runs can have no associated pull_requests. Read the original PR number and base ref
    // recorded by check-wire, then verify them against the live PR before making any changes.
    const checkout = only(wire.steps.filter(step => step.conclusion === 'success'
        && step.name.startsWith('Check out target commit for PR #')));
    const target = checkout?.name.match(/^Check out target commit for PR #([1-9]\d*) into (.+)$/u);
    if (!target || !run.head_repository)
        return core.info('Could not identify the PR and its target branch.');

    const [, number, baseRef] = target;
    const issue = {...repo, issue_number: Number(number)};
    const comments = await github.paginate(issues.listComments, {...issue, per_page: 100});
    const comment = comments.find(c => c.user?.login === bot && c.body?.startsWith(marker));
    const events = upload === 'skipped' ? await github.paginate(issues.listEvents, {...issue, per_page: 100}) : [];

    const {data: pr} = await pulls.get({...repo, pull_number: issue.issue_number});
    if (pr.state !== 'open' || pr.head.repo?.id !== run.head_repository.id || pr.head.ref !== run.head_branch
        || pr.head.sha !== run.head_sha || pr.base.ref !== baseRef)
        return core.info('The PR has closed or changed since the comparison.');

    const labeled = pr.labels.some(l => l.name === label);
    if (upload === 'skipped') {
        const last = events.findLast(e => e.label?.name === label && ['labeled', 'unlabeled'].includes(e.event));
        if (labeled && last?.event === 'labeled' && last.actor?.login === bot)
            await issues.removeLabel({...issue, name: label});

        if (comment)
            await issues.deleteComment({...repo, comment_id: comment.id});
        return;
    }

    if (!labeled)
        await issues.addLabels({...issue, labels: [label]});

    const url = `${context.serverUrl}/${repo.owner}/${repo.repo}/actions/runs/${run.id}/attempts/${wire.run_attempt}#summary-${wire.id}`;
    const body = `${marker}\nWire format changed. Review the [wire diff in Job Summary](${url}) for rolling upgrade compatibility.`;
    if (!comment)
        await issues.createComment({...issue, body});
    else if (comment.body !== body)
        await issues.updateComment({...repo, comment_id: comment.id, body});
};
