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

const assert = require('node:assert/strict');
const test = require('node:test');
const report = require('./report-wire-changes');

const bot = {login: 'github-actions[bot]'};
const reviewer = {login: 'reviewer'};
const success = name => ({name, conclusion: 'success', status: 'completed'});

function fixture() {
    const run = {id: 12, run_attempt: 1, head_sha: 'abc', head_branch: 'topic', head_repository: {id: 5}, pull_requests: []};
    const context = {repo: {owner: 'apache', repo: 'ignite'}, serverUrl: 'https://github.com', payload: {workflow_run: run}};
    const pr = {state: 'open', head: {sha: 'abc', ref: 'topic', repo: {id: 5}}, base: {ref: 'master'}, labels: []};
    const complete = success('Check all wire format descriptions are collected');
    const compare = success('Compare descriptions and publish summary');
    const upload = success('Upload wire diff');
    const checkout = success('Check out target commit for PR #42 into master');
    const wire = {id: 21, name: 'Compare wire format descriptions', steps: [checkout, compare, upload],
        head_sha: 'abc', run_attempt: 1, conclusion: 'success'};
    const java = {name: 'Check java code on JDK 17', steps: [complete], head_sha: 'abc', run_attempt: 1};
    const jobs = [java, wire];
    const comments = [];
    const events = [];
    const label = {name: 'compatibility'};

    function applyLabel(user) {
        pr.labels.push(label);
        events.push({label, event: 'labeled', actor: user});
    }

    function removeLabel(user) {
        pr.labels = [];
        events.push({label, event: 'unlabeled', actor: user});
    }

    const issues = {
        listComments: () => comments,
        listEvents: () => events,
        createComment: ({body}) => comments.push({id: 99, user: bot, body}),
        updateComment: ({comment_id, body}) => { comments.find(c => c.id === comment_id).body = body; },
        deleteComment: ({comment_id}) => comments.splice(comments.findIndex(c => c.id === comment_id), 1),
        addLabels: () => applyLabel(bot),
        removeLabel: () => removeLabel(bot),
    };
    const actions = {listJobsForWorkflowRun: () => jobs};
    const pulls = {get: ({pull_number}) => {
        assert.equal(pull_number, 42);
        return {data: pr};
    }};
    const github = {rest: {actions, pulls, issues}, paginate: (method, params) => method(params)};
    const publish = () => report({github, context, core: {info() {}}});

    return {run, pr, complete, compare, checkout, upload, jobs, wire, java, comments, events, issues, applyLabel, removeLabel, publish};
}

test('fork PR: reports a diff, updates the same comment on rerun and cleans up an empty diff', async () => {
    const f = fixture();
    await f.publish();
    assert.deepEqual(f.pr.labels, [{name: 'compatibility'}]);
    const [comment] = f.comments;
    assert.match(comment.body, /actions\/runs\/12\/attempts\/1#summary-21/);

    f.removeLabel(reviewer);
    f.run.run_attempt++;
    f.jobs.push({...f.wire, id: 22, run_attempt: 2});
    await f.publish();
    assert.equal(f.events.at(-1).actor, bot);
    assert.deepEqual(f.comments, [comment]);
    assert.match(comment.body, /actions\/runs\/12\/attempts\/2#summary-22/);

    f.upload.conclusion = 'skipped';
    await f.publish();
    assert.deepEqual(f.comments, []);
    assert.deepEqual(f.pr.labels, []);
});

for (const reapplied of [false, true]) {
    test(`preserves a manual label (reapplied: ${reapplied}) and a reviewer's marked comment`, async () => {
        const f = fixture();
        const manual = {id: 1, user: reviewer, body: '<!-- ignite-rolling-upgrade-check -->'};
        f.comments.push(manual);
        if (reapplied) {
            await f.publish();
            f.removeLabel(reviewer);
        }
        f.applyLabel(reviewer);
        await f.publish();
        assert.equal(f.events.at(-1).actor, reviewer);
        f.upload.conclusion = 'skipped';
        await f.publish();
        assert.deepEqual(f.pr.labels, [{name: 'compatibility'}]);
        assert.deepEqual(f.comments, [manual]);
    });
}

test('publishes after the target advances or a later Java step fails', async () => {
    const f = fixture();
    f.pr.base.sha = 'new-target-commit';
    f.java.conclusion = 'failure';
    f.run.conclusion = 'failure';
    await f.publish();
    assert.equal(f.comments.length, 1);
});

test('adopts the previous reporter comment and preserves its ID', async () => {
    const f = fixture();
    f.comments.push({id: 31, user: bot, body: '<!-- ignite-rolling-upgrade-check -->\nOld report'});
    await f.publish();
    assert.equal(f.comments.length, 1);
    assert.equal(f.comments[0].id, 31);
    assert.match(f.comments[0].body, /Job Summary/);
});

test('recovers when labeling succeeds but posting the comment fails', async () => {
    const f = fixture();
    const create = f.issues.createComment;
    f.issues.createComment = () => { throw new Error('Request failed'); };
    await assert.rejects(f.publish(), /Request failed/);
    f.issues.createComment = create;
    await f.publish();
    assert.equal(f.events.length, 1);
    assert.equal(f.comments.length, 1);
});

const unverified = {
    'failed completeness': f => { f.complete.conclusion = 'failure'; },
    'skipped completeness': f => { f.complete.conclusion = 'skipped'; },
    'missing completeness': f => { f.java.steps = []; },
    'failed wire job': f => { f.wire.conclusion = 'failure'; },
    'skipped comparison': f => { f.compare.conclusion = 'skipped'; },
    'cancelled upload': f => { f.upload.conclusion = 'cancelled'; },
    'missing upload': f => { f.wire.steps.pop(); },
    'newer PR head': f => { f.pr.head.sha = 'def'; },
    'different source repository': f => { f.pr.head.repo.id++; },
    'different source branch': f => { f.pr.head.ref = 'another'; },
    'retargeted PR': f => { f.pr.base.ref = 'release'; },
    'closed PR': f => { f.pr.state = 'closed'; },
    'missing original PR target': f => { f.checkout.name = 'Check out target commit'; },
    'superseded attempt': f => { f.jobs.push({...f.wire, run_attempt: 2}); },
};

for (const [scenario, change] of Object.entries(unverified)) {
    test(`leaves existing report alone: ${scenario}`, async () => {
        const f = fixture();
        await f.publish();
        const before = structuredClone(f.comments);
        f.upload.conclusion = 'skipped';
        change(f);
        await f.publish();
        assert.deepEqual(f.comments, before);
        assert.deepEqual(f.pr.labels, [{name: 'compatibility'}]);
        assert.equal(f.events.length, 1);
    });
}
