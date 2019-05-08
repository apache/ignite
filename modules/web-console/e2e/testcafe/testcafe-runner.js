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

const { startEnv, dropTestDB, insertTestUser } = require('./environment/envtools');

const createTestCafe = require('testcafe');

let testcafe = null;

const startTestcafe = (config) => {
    return createTestCafe('localhost', 1337, 1338)
        .then(async(tc) => {
            try {
                if (config.enableEnvironment)
                    await startEnv();

                await dropTestDB();
                await insertTestUser();

                testcafe = tc;

                const runner = testcafe.createRunner();

                console.log('Start E2E testing!');

                return runner
                    .src(config.fixturesPathsArray)
                    .browsers(config.browsers)
                    .reporter(config.reporter)
                    .run({ skipJsErrors: true });
            } catch (err) {
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
