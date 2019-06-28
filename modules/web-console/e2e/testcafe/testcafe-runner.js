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

const { dropTestDB } = require('./environment/envtools');

const createTestCafe = require('testcafe');

let testcafe = null;

const startTestcafe = (config) => {
    return createTestCafe('localhost', 1337, 1338)
        .then((tc) => {
            try {
                testcafe = tc;

                const runner = testcafe.createRunner();

                console.log('Start E2E testing!');

                return runner
                    .src(config.fixturesPathsArray)
                    .browsers(config.browsers)
                    .reporter(config.reporter)
                    .run({ skipJsErrors: true });
            }
            catch (err) {
                console.log(err);

                process.exit(1);
            }
        })
        .then(async(failedCount) => {
            console.log('Cleaning after tests...');

            testcafe.close();

            await dropTestDB();

            console.log('Tests failed: ' + failedCount);
        });
};

module.exports = { startTestcafe };
