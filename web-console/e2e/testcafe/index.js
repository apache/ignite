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

const glob = require('glob');
const { startTestcafe } = require('./testcafe-runner');

// See all supported browsers at http://devexpress.github.io/testcafe/documentation/using-testcafe/common-concepts/browsers/browser-support.html#locally-installed-browsers
const BROWSERS = ['chromium:headless --no-sandbox']; // For example: ['chrome', 'firefox'];

const FIXTURES_PATHS = glob.sync('./fixtures/**/*.js');

const testcafeRunnerConfig = {
    browsers: BROWSERS,
    reporter: process.env.REPORTER || 'spec',
    fixturesPathsArray: FIXTURES_PATHS
};

startTestcafe(testcafeRunnerConfig).then(() => {
    process.exit(0);
});
