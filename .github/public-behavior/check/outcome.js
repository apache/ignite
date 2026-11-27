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

// Outcome of the check, following the decision tree of IGNITE-29326.

'use strict';

/** Outcomes that fail the check. */
const FAILED = ['rfc-name', 'rfc-missing', 'error'];

/**
 * @param rfcs RFC files of the PR and RFC links in its description, see rfc.js.
 * @param checkOnly Whether the PR changes only the check: then its findings show what the check describes differently.
 * @return Outcome for the findings of compare.js.
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

module.exports = { FAILED, decide };
