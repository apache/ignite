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

const glob = require('glob');
const argv = require('minimist')(process.argv.slice(2));
const { startTestcafe } = require('./testcafe-runner');

const enableEnvironment = argv.env;

// See all supported browsers at http://devexpress.github.io/testcafe/documentation/using-testcafe/common-concepts/browsers/browser-support.html#locally-installed-browsers
const BROWSERS = ['chromium:headless --no-sandbox']; // For example: ['chrome', 'firefox'];

const FIXTURES_PATHS = glob.sync('./fixtures/**/*.js');

const testcafeRunnerConfig = {
    browsers: BROWSERS,
    enableEnvironment: enableEnvironment || false,
    reporter: process.env.REPORTER || 'spec',
    fixturesPathsArray: FIXTURES_PATHS
};

startTestcafe(testcafeRunnerConfig).then(() => {
    process.exit(0);
});
