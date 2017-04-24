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
const testDomains = require('../data/domains.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let domainService;
let mongo;
let errors;
let db;

suite('DomainsServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/domains'),
            injector('mongo'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_domainService, _mongo, _errors, _db]) => {
                mongo = _mongo;
                domainService = _domainService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Create new domain', (done) => {
        const dupleDomain = Object.assign({}, testDomains[0], {valueType: 'other.Type'});

        delete dupleDomain._id;

        domainService.batchMerge([dupleDomain])
            .then((results) => {
                const domain = results.savedDomains[0];

                assert.isObject(domain);
                assert.isDefined(domain._id);

                return mongo.DomainModel.findById(domain._id);
            })
            .then((domain) => {
                assert.isObject(domain);
                assert.isDefined(domain._id);
            })
            .then(done)
            .catch(done);
    });

    test('Update existed domain', (done) => {
        const newValType = 'value.Type';

        const domainBeforeMerge = Object.assign({}, testDomains[0], {valueType: newValType});

        domainService.batchMerge([domainBeforeMerge])
            .then(({savedDomains, generatedCaches}) => {
                assert.isArray(savedDomains);
                assert.isArray(generatedCaches);

                assert.equal(1, savedDomains.length);
                assert.equal(0, generatedCaches.length);

                return mongo.DomainModel.findById(savedDomains[0]._id);
            })
            .then((domainAfterMerge) =>
                assert.equal(domainAfterMerge.valueType, newValType)
            )
            .then(done)
            .catch(done);
    });

    test('Create duplicated domain', (done) => {
        const dupleDomain = Object.assign({}, testDomains[0]);

        delete dupleDomain._id;

        domainService.batchMerge([dupleDomain])
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed domain', (done) => {
        domainService.remove(testDomains[0]._id)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 1)
            )
            .then(() => mongo.DomainModel.findById(testDomains[0]._id))
            .then((notFoundDomain) =>
                assert.isNull(notFoundDomain)
            )
            .then(done)
            .catch(done);
    });

    test('Remove domain without identifier', (done) => {
        domainService.remove()
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed domain', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        domainService.remove(validNoExistingId)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 0)
            )
            .then(done)
            .catch(done);
    });

    test('Get all domains by space', (done) => {
        domainService.listBySpaces(testSpaces[0]._id)
            .then((domains) =>
                assert.equal(domains.length, 5)
            )
            .then(done)
            .catch(done);
    });

    test('Remove all domains in space', (done) => {
        domainService.removeAll(testAccounts[0]._id, false)
            .then(({rowsAffected}) =>
                assert.equal(rowsAffected, 5)
            )
            .then(done)
            .catch(done);
    });

    test('Update linked entities on update domain', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove domain', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove all domains in space', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });
});
