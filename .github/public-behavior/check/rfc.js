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

// Rules of the RFC process that the check applies: names of RFC files, RFC files of the PR and RFC links in its description.

'use strict';

const RFC_NAME = /^rfcs\/IGNITE-\d+-[a-z0-9]+(?:-[a-z0-9]+)*\.md$/;
const RFC_FORMAT = 'rfcs/IGNITE-<ticket>-<short-title>.md';
const RFC_EXAMPLE = 'rfcs/IGNITE-12345-cache-timeout.md';
const RFC_PATH = /^(.+?)\/(rfcs\/.+)$/;
const TEMPLATE = 'rfcs/TEMPLATE.md';

/** RFCs of a PR that needs none. */
const NO_RFC = { files: [], invalidNames: [], linked: [], badLinks: [] };

/**
 * @param names Output of `git diff --no-renames --name-only --diff-filter=d -z` between the target branch and the PR merge for
 *      rfcs/: the paths of the added and updated files, including the new path of a renamed one.
 * @return RFC files that the PR adds or updates and the ones with invalid names.
 */
function rfcFiles(names) {
    const files = names.split('\0').filter(f => f.endsWith('.md') && f !== TEMPLATE);

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

module.exports = { RFC_NAME, RFC_FORMAT, RFC_EXAMPLE, NO_RFC, rfcFiles, rfcLinks };
