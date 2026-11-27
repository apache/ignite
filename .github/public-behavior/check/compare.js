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

// Compares the declarations of the target branch and the PR merge. README.md describes the format of the declarations.

'use strict';

/** Parses the declarations printed by describe/describe.sh. Columns that an older version does not print are empty. */
function parse(text) {
    const res = new Map();

    for (const line of text.split('\n')) {
        if (!line)
            continue;

        const [area, key, value = '', flags = '', location = ''] = line.split('\t');

        res.set(area + '\t' + key, { area, key, value, flags: flags.split(',').filter(Boolean), location });
    }

    return res;
}

/**
 * @return Declarations added, removed or changed by the PR. A removed deprecated declaration has its own kind: the reviewer
 *      decides whether it needs an RFC. A removed declaration points at its location on the target branch, any other at its
 *      location in the PR.
 */
function compare(baseText, mergeText) {
    const base = parse(baseText);
    const merge = parse(mergeText);
    const res = [];

    const finding = (b, m) => res.push({
        area: (b || m).area,
        key: (b || m).key,
        before: b ? b.value : null,
        after: m ? m.value : null,
        kind: !m && b.flags.includes('deprecated') ? 'deprecated-removal' : 'contract',
        location: (m || b).location,
    });

    for (const [id, b] of base) {
        const m = merge.get(id);

        if (!m || m.value !== b.value)
            finding(b, m);
    }

    for (const [id, m] of merge) {
        if (!base.has(id))
            finding(null, m);
    }

    return res.sort((a, b) => a.area.localeCompare(b.area) || a.key.localeCompare(b.key));
}

module.exports = { compare };
