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

// Outcome of the check and the result.json that carries it from the PR job to the job that comments on the PR.

'use strict';

const { RFC_NAME } = require('./rfc.js');

const KINDS = ['contract', 'deprecated-removal', 'experimental', 'possible'];
const OUTCOMES = ['clean', 'review', 'check', 'rfc-found', 'rfc-name', 'rfc-missing', 'error'];
const FAILED = ['rfc-name', 'rfc-missing', 'error'];
const NEEDS_RFC = ['rfc-found', 'rfc-name', 'rfc-missing'];
const STEPS = ['checkout', 'tests', 'java', 'base', 'merge', 'report', 'result'];
const LINK_PROBLEMS = ['name', 'missing'];
const BLOB = /^[0-9a-f]{40,64}$/;

/** Longest list of RFC files or links in a result. A longer list is cut after the outcome is decided. */
const SHOWN_FILES = 20;

/**
 * @param checkOnly Whether the PR changes only the check: then its findings show what the check describes differently.
 * @return Outcome for described declarations, following the decision tree of IGNITE-29326.
 */
function decide(findings, rfcs, checkOnly = false) {
    if (findings.length === 0)
        return { outcome: 'clean' };

    const res = { findings, rfcs: [], invalidNames: [], badLinks: [] };

    if (checkOnly)
        return { outcome: 'check', ...res };

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

/** @return Result whose lists of RFC files and links are cut to {@link SHOWN_FILES}, with the number of entries left out. */
function shorten(res) {
    const cut = { ...res };
    let notShown = 0;

    for (const shown of ['rfcs', 'invalidNames', 'badLinks']) {
        if (cut[shown]) {
            notShown += Math.max(0, cut[shown].length - SHOWN_FILES);
            cut[shown] = cut[shown].slice(0, SHOWN_FILES);
        }
    }

    return notShown > 0 ? { ...cut, notShown } : cut;
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

    if (res.outcome === 'review' ? contract : NEEDS_RFC.includes(res.outcome) && !contract)
        fail('outcome');

    const rfc = r => RFC_NAME.test(r.path) && ['pr', 'link'].includes(r.source) && (r.blob === undefined || BLOB.test(r.blob));

    if (!list(res.rfcs, SHOWN_FILES, rfc) || (res.outcome === 'rfc-found') !== (res.rfcs.length > 0))
        fail('rfcs');

    if (!list(res.invalidNames, SHOWN_FILES, p => str(p, 300)) || (res.outcome === 'rfc-name') !== (res.invalidNames.length > 0))
        fail('invalidNames');

    if (!list(res.badLinks, SHOWN_FILES, l => str(l.path, 300) && LINK_PROBLEMS.includes(l.reason)))
        fail('badLinks');

    if (res.notShown !== undefined && !(Number.isInteger(res.notShown) && res.notShown > 0))
        fail('notShown');

    return {
        outcome: res.outcome,
        findings: res.findings.map(({ area, key, before, after, kind, location, hint }) =>
            ({ area, key, before, after, kind, location, ...(hint === undefined ? {} : { hint }) })),
        rfcs: res.rfcs.map(({ path, source, blob }) => ({ path, source, ...(blob === undefined ? {} : { blob }) })),
        invalidNames: [...res.invalidNames],
        badLinks: res.badLinks.map(({ path, reason }) => ({ path, reason })),
        ...(res.notShown === undefined ? {} : { notShown: res.notShown }),
    };
}

module.exports = { FAILED, decide, shorten, validate };
