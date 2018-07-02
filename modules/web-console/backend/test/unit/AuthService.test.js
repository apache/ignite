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
const testAccounts = require('../data/accounts.json');

let authService;
let errors;
let db;

suite('AuthServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/auth'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_authService, _errors, _db]) => {
                authService = _authService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Reset password token for non existing user', (done) => {
        authService.resetPasswordToken('non-exisitng@email.ee')
            .catch((err) => {
                assert.instanceOf(err, errors.MissingResourceException);
                done();
            });
    });

    test('Reset password token for existing user', (done) => {
        authService.resetPasswordToken(testAccounts[0].email)
            .then((account) => {
                assert.notEqual(account.resetPasswordToken.length, 0);
                assert.notEqual(account.resetPasswordToken, testAccounts[0].resetPasswordToken);
            })
            .then(done)
            .catch(done);
    });

    test('Reset password by token for non existing user', (done) => {
        authService.resetPasswordByToken('0')
            .catch((err) => {
                assert.instanceOf(err, errors.MissingResourceException);
                done();
            });
    });

    test('Reset password by token for existing user', (done) => {
        authService.resetPasswordByToken(testAccounts[0].resetPasswordToken, 'NewUniquePassword$1')
            .then((account) => {
                assert.isUndefined(account.resetPasswordToken);
                assert.notEqual(account.hash, 0);
                assert.notEqual(account.hash, testAccounts[0].hash);
            })
            .then(done)
            .catch(done);
    });

    test('Validate user for non existing reset token', (done) => {
        authService.validateResetToken('Non existing token')
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalAccessError);
                done();
            });
    });

    test('Validate reset token', (done) => {
        authService.validateResetToken(testAccounts[0].resetPasswordToken)
            .then(({token, email}) => {
                assert.equal(email, testAccounts[0].email);
                assert.equal(token, testAccounts[0].resetPasswordToken);
            })
            .then(done)
            .catch(done);
    });
});
