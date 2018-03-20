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
const path = require('path');

require('app-module-path').addPath(path.join(__dirname, 'node_modules'));
require('app-module-path').addPath(__dirname);

const argv = require('minimist')(process.argv.slice(2));
const envEnabled = argv.env;

const { startEnv, dropTestDB } = require('./envtools');

const createTestCafe = require('testcafe');

// See all supported browsers at http://devexpress.github.io/testcafe/documentation/using-testcafe/common-concepts/browsers/browser-support.html#locally-installed-browsers
const BROWSERS = ['chromium:headless --no-sandbox']; // For example: ['chrome', 'firefox'];

let testcafe = null;

const resolveFixturesPaths = () => {
    let fixturesPaths = glob.sync(' ./fixtures/**/*.js');

    if (process.env.IGNITE_MODULES) {
        const igniteModulesTestcafe = path.join(process.env.IGNITE_MODULES, 'e2e/testcafe');
        const additionalFixturesPaths = glob.sync(path.join(igniteModulesTestcafe, 'fixtures', '**/*.js'));
        const relativePaths = new Set(additionalFixturesPaths.map((fixturePath) => path.relative(igniteModulesTestcafe, fixturePath)));

        fixturesPaths = fixturesPaths.filter((fixturePath) => !relativePaths.has(path.relative(process.cwd(), fixturePath))).concat(additionalFixturesPaths);
    }

    return fixturesPaths;
};

createTestCafe('localhost', 1337, 1338)
    .then(async(tc) => {
        try {
            if (envEnabled)
                await startEnv();

            await dropTestDB();

            testcafe = tc;

            const runner = testcafe.createRunner();
            const reporter = process.env.TEAMCITY ? 'teamcity' : 'spec';

            console.log('Start E2E testing!');

            return runner
                .src(resolveFixturesPaths())
                .browsers(BROWSERS)
                .reporter(reporter)
                .run({ skipJsErrors: true, quarantineMode: true });
        } catch (err) {
            console.log(err);

            process.exit(1);
        }
    })
    .then(async(failedCount) => {
        console.log('Cleaning after tests...');

        testcafe.close();

        if (envEnabled)
            await dropTestDB();

        console.log('Tests failed: ' + failedCount);

        process.exit(0);
    });
