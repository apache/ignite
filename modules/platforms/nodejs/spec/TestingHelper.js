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

'use strict';

require('jasmine-expect');

const config = require('./config');

const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const Errors = IgniteClient.Errors;

const TIMEOUT_MS = 20000;

// Helper class for testing apache-ignite-client library.
// Contains common methods for testing environment initialization and cleanup.
class TestingHelper {
    static get TIMEOUT() {
        return TIMEOUT_MS;
    }

    // Initializes testing environment: creates and starts the library client, sets default jasmine test timeout.
    // Should be called from any test suite beforeAll method.
    static async init() {
        jasmine.DEFAULT_TIMEOUT_INTERVAL = TIMEOUT_MS;

        TestingHelper._igniteClient = new IgniteClient();
        TestingHelper._igniteClient.setDebug(config.debug);
        await TestingHelper._igniteClient.connect(new IgniteClientConfiguration(...config.endpoints));
    }

    // Cleans up testing environment.
    // Should be called from any test suite afterAll method.
    static async cleanUp() {
        await TestingHelper.igniteClient.disconnect();
    }

    static get igniteClient() {
        return TestingHelper._igniteClient;
    }

    static async destroyCache(cacheName, done) {
        try {
            await TestingHelper.igniteClient.destroyCache(cacheName);
        }
        catch (err) {
            TestingHelper.checkOperationError(err, done);
        }
    }

    static checkOperationError(error, done) {
        TestingHelper.checkError(error, Errors.OperationError, done)
    }

    static checkIllegalArgumentError(error, done) {
        TestingHelper.checkError(error, Errors.IgniteClientError, done)
    }

    static checkError(error, errorType, done) {
        if (!(error instanceof errorType)) {
            done.fail('unexpected error: ' + error);
        }
    }

    static logDebug(message) {
        if (config.debug) {
            console.log(message);
        }
    }
}

module.exports = TestingHelper;
