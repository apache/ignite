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
const line = (area, key, value, flags = '', location = 'A.java:1') => [area, key, value, flags, location].join('\t');

/** @return Line of a system property. */
const property = (name, value, flags) => line('Configuration', 'system property ' + name, value, flags);

/** Description of the target branch. */
const BASE = [
    property('IGNITE_QUIET', 'Boolean, default true'),
    property('IGNITE_UPDATE_NOTIFIER', 'Boolean, default true'),
    property('IGNITE_EVICTION_PERMITS', 'Integer, deprecated', 'deprecated'),
].join('\n');

const CONTRACT = { area: 'Configuration', key: 'system property IGNITE_NEW', before: null, after: 'Boolean, default false', kind: 'contract', location: 'A.java:1' };
const DEPRECATED = { area: 'Configuration', key: 'system property IGNITE_EVICTION_PERMITS', before: 'Integer, deprecated', after: null, kind: 'deprecated-removal', location: 'A.java:1' };

/** Target repository of the PRs in the tests. */
const APACHE = { owner: { login: 'apache' }, name: 'ignite', full_name: 'apache/ignite', html_url: 'https://github.com/apache/ignite' };

module.exports = { line, property, BASE, CONTRACT, DEPRECATED, APACHE };
