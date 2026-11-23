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
const RFC_PATH = /^.+?\/(rfcs\/.+)$/;
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
 * Finds direct links to RFC files in the PR description. A link counts if its whole path is an RFC file on the target branch.
 * The branch, tag or commit in the link does not matter: the report links the file on the target branch.
 *
 * @param body PR description.
 * @param repo Target repository, such as {@code apache/ignite}.
 * @param onTarget Checks that a file exists on the target branch.
 */
function rfcLinks(body, repo, onTarget) {
    const prefix = `https://github.com/${repo}/`.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(prefix + '(?:blob|tree)/([^\\s<>"\'()\\[\\]]+)', 'g');
    const linked = [];
    const badLinks = [];

    for (const [, rest] of (body || '').matchAll(pattern)) {
        const file = RFC_PATH.exec(rest.replace(/[.,;:!]+$/, '').replace(/[?#].*$/, ''))?.[1];

        if (!file || linked.includes(file) || badLinks.some(l => l.path === file))
            continue;

        const reason = !RFC_NAME.test(file) ? 'name' : !onTarget(file) ? 'missing' : null;

        if (reason)
            badLinks.push({ path: file, reason });
        else
            linked.push(file);
    }

    return { linked, badLinks };
}

module.exports = { RFC_NAME, RFC_FORMAT, RFC_EXAMPLE, NO_RFC, rfcFiles, rfcLinks };
