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

        const [area, key, value = '', flags = '', location = '', group = '', deprecation = ''] = line.split('\t');

        res.set(area + '\t' + key, { area, key, value, flags: flags.split(',').filter(Boolean), location, group, deprecation });
    }

    return res;
}

/**
 * @return Keys of the declarations whose rename group only changed its name: the group left the target branch, another group
 *      came in the PR, and their declarations are the same apart from the group name. A group is identified by its name and
 *      the part of the key before the name, such as the group of a code: two groups may have codes with the same name.
 */
function renamedKeys(base, merge) {
    const groupId = d => d.area + '\t' + d.key.slice(0, d.key.indexOf(d.group)) + '\t' + d.group;

    const groups = (decls, other) => {
        const present = new Set([...other.values()].filter(d => d.group).map(groupId));
        const res = new Map();

        for (const [id, d] of decls) {
            const gid = groupId(d);

            if (!d.group || present.has(gid))
                continue;

            if (!res.has(gid))
                res.set(gid, { ids: [], parts: [] });

            res.get(gid).ids.push(id);
            res.get(gid).parts.push(d.key.replace(d.group, '\u0001') + '\u0000' + d.value);
        }

        return [...res.values()].map(g => ({ ids: g.ids, sig: g.ids[0].split('\t')[0] + '\u0002' + g.parts.sort().join('\u0002') }));
    };

    const added = groups(merge, base);
    const res = new Set();

    for (const left of groups(base, merge)) {
        const i = added.findIndex(g => g.sig === left.sig);

        if (i >= 0) {
            [...left.ids, ...added[i].ids].forEach(id => res.add(id));
            added.splice(i, 1);
        }
    }

    return res;
}

/**
 * @return Declarations added, removed or changed by the PR. A removed deprecated declaration and any change to an experimental
 *      declaration have their own kinds: the reviewer decides whether they need an RFC. A removed deprecated declaration points
 *      at the annotation that deprecates it, which can be on its type or parent command; any other finding points at the
 *      declaration.
 */
function compare(baseText, mergeText) {
    const base = parse(baseText);
    const merge = parse(mergeText);
    const renamed = renamedKeys(base, merge);
    const res = [];

    const finding = (b, m) => {
        const kind = !m && b.flags.includes('deprecated') ? 'deprecated-removal'
            : (b || m).flags.includes('experimental') ? 'experimental' : 'contract';

        res.push({
            area: (b || m).area,
            key: (b || m).key,
            before: b ? b.value : null,
            after: m ? m.value : null,
            kind,
            location: kind === 'deprecated-removal' ? b.deprecation : (m || b).location,
        });
    };

    for (const [id, b] of base) {
        const m = merge.get(id);

        if (!renamed.has(id) && (!m || m.value !== b.value))
            finding(b, m);
    }

    for (const [id, m] of merge) {
        if (!renamed.has(id) && !base.has(id))
            finding(null, m);
    }

    return res.sort((a, b) => a.area.localeCompare(b.area) || a.key.localeCompare(b.key));
}

module.exports = { compare };
