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

// Markdown report of a validated result, for the job summary and the PR comment. Everything that comes from the PR is
// formatted so that it cannot break out of its code span or block.

'use strict';

const { RFC_FORMAT, RFC_EXAMPLE } = require('./rfc.js');

const PROCESS_URL = 'https://github.com/apache/ignite/tree/master/rfcs';
const CONTRACT_URL = 'https://github.com/apache/ignite/blob/master/rfcs/IGNITE-29219-public-api-and-compatibility.md';
const CHANGING_URL = 'https://github.com/apache/ignite/blob/master/.github/public-behavior/README.md#changing-the-check';
const LINE_CHARS = 500;

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
    const note = { 'deprecated-removal': ` [${f.hint || 'deprecated'}]`, 'experimental': ' [experimental]', 'possible': ' [possible]' }[f.kind] || '';
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
        'review': 'Review deprecated, experimental or possible changes',
        'check': 'Review the changes to the check',
        'rfc-found': 'Review the RFC',
        'rfc-name': 'Fix the RFC filename',
        'rfc-missing': 'RFC needed',
        'error': 'Check could not finish',
    }[res.outcome];
}

/** @return Explanation of a check that could not finish. */
function failure(step) {
    return {
        'tests': 'The tests of the check fail. Fix the check in this PR.',
        'java': 'The check could not set up Java. Rerun the check.',
        'base': 'The check could not build the target branch or describe its public declarations. Rerun the check. ' +
            'If it fails again, the check needs a fix on the target branch.',
        'merge': 'The check could not build the PR merged into the target branch or describe its public declarations. ' +
            'If the PR does not compile, fix it. If the check log points to `.github/public-behavior/describe`, the PR changes ' +
            `code that the check reads: [update the check](${CHANGING_URL}) in this PR. Otherwise rerun the check.`,
    }[step] || 'The check failed for a technical reason. Rerun the check.';
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
        out.push(failure(res.step));

        if (links.runUrl)
            out.push('', `The [check log](${links.runUrl}) shows the cause.`);

        return out.join('\n');
    }

    const notShown = res.notShown ? `${res.notShown} more` : '';
    const notShownLine = notShown ? [`- ${notShown} not shown.`] : [];

    if (res.outcome === 'review') {
        out.push('This PR removes deprecated declarations, changes experimental API or possibly changes a message or record ' +
            `layout. The [RFC process](${PROCESS_URL}) lets authors remove APIs deprecated before it without an RFC, and ` +
            'experimental APIs may change in any release. A change marked `[possible]` touches code that the check could not ' +
            'resolve, so the layout may stay the same.',
            '',
            'Reviewer: decide whether these changes need an RFC. For a deprecated declaration, check when it was deprecated. ' +
            'For a possible change, check whether the bytes on the wire or on disk change.');
    }
    else if (res.outcome === 'check') {
        out.push('This PR changes only the check. The target branch is described by its own version of the check and this PR by ' +
            'the new one, so the changes below are declarations that the new version describes differently.',
            '',
            'Reviewer: check that the check still describes everything that users rely on, and describes it correctly.');
    }
    else if (res.outcome === 'rfc-found') {
        out.push(`RFC: ${res.rfcs.map(r => `[${r.path}](${rfcUrl(r, links)})`).join(', ')}${notShown ? ` and ${notShown}` : ''}.`,
            '',
            'Reviewer: check that the RFC covers the changes below, review compatibility and verify any required vote on the dev list.');
    }
    else if (res.outcome === 'rfc-name') {
        out.push(...res.invalidNames.map(p => `- ${code(p)} does not match ${code(RFC_FORMAT)}.`),
            ...notShownLine,
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
                name: `does not match ${code(RFC_FORMAT)}.`,
                missing: 'is not on the target branch.',
            }[l.reason]), ...notShownLine);
        }
    }

    out.push('', 'Detected changes (`-` target branch, `+` this PR):', '', renderFindings(res.findings, limit, links.runUrl));

    return out.join('\n').trimEnd();
}

module.exports = { title, render };
