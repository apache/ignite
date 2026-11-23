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

// Declarations, findings and repositories that the tests of several modules share.

'use strict';

/** @return Declaration line in the format of describe/describe.sh. */
const line = (area, key, value, flags = '', location = 'A.java:1', group = '', deprecation = '') =>
    [area, key, value, flags, location, group, deprecation].join('\t');

/** @return Line of an internal constant {@code Class.NAME}, whose name is a rename group. */
const constant = (area, key, value) => line(area, key, value, '', 'A.java:1', key.split('.').pop());

/** @return Line of a system view column, whose row class is a rename group. */
const column = (view, name, value) => line('Monitoring', `system view ${view} column ${name}`, value, '', 'A.java:1', view);

/** Description of the target branch. */
const BASE = [
    line('CLI', 'control.sh --cache list --groups', 'optional, boolean'),
    column('CacheView', 'cacheName', 'position 1, java.lang.String'),
    column('CacheView', 'cacheId', 'position 2, int'),
    line('Java API', 'org.apache.ignite.Old#run()', 'public void run()', 'deprecated'),
    line('Java API', 'org.apache.ignite.Exp#run()', 'public void run()', 'experimental'),
].join('\n');

const CONTRACT = { area: 'CLI', key: 'control.sh --cache list --seq', before: null, after: 'optional, boolean', kind: 'contract', location: 'A.java:1' };
const DEPRECATED = { area: 'Java API', key: 'org.apache.ignite.Old#run()', before: 'public void run()', after: null, kind: 'deprecated-removal', location: 'A.java:1' };

/** Target repository of the PRs in the tests. */
const APACHE = { owner: { login: 'apache' }, name: 'ignite', full_name: 'apache/ignite', html_url: 'https://github.com/apache/ignite' };

module.exports = { line, constant, column, BASE, CONTRACT, DEPRECATED, APACHE };
