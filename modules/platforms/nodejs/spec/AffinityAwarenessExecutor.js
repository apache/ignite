/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const Jasmine = require('jasmine');

const jasmine = new Jasmine();
jasmine.loadConfig({
    'spec_dir': 'spec',
    'spec_files': [
        "affinity_awareness/**/*[sS]pec.js",
	    "cache/**/*[sS]pec.js",
	    "query/**/*[sS]pec.js"
    ],
    "random": false,
    // We want to stop immediately if there are not enough nodes in cluster, for example
    "stopOnSpecFailure": true
});
jasmine.execute();
