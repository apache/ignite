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

const assert = require('chai').assert;
const injector = require('../injector');

let utils;
let errors;
let db;

suite('UtilsTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/utils'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_utils, _errors, _db]) => {
                utils = _utils;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Check token generator', () => {
        const tokenLength = 16;
        const token1 = utils.randomString(tokenLength);
        const token2 = utils.randomString(tokenLength);

        assert.equal(token1.length, tokenLength);
        assert.equal(token2.length, tokenLength);
        assert.notEqual(token1, token2);
    });
});
