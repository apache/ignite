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

let domainService;
let mongo;
let errors;

suite('DomainsServiceTestsSuite', () => {
    const prepareUserSpaces = () => {
        return mongo.Account.create(testAccounts)
            .then((accounts) => {
                return Promise.all(accounts.map((account) => mongo.Space.create(
                    [
                        {name: 'Personal space', owner: account._id, demo: false},
                        {name: 'Demo space', owner: account._id, demo: true}
                    ]
                )))
                    .then((spaces) => [accounts, spaces]);
            });
    };

    suiteSetup(() => {
        return Promise.all([injector('services/domains'),
            injector('mongo'),
            injector('errors')])
            .then(([_domainService, _mongo, _errors]) => {
                mongo = _mongo;
                domainService = _domainService;
                errors = _errors;
            });
    });

    setup(() => {
        return Promise.all([
            mongo.DomainModel.remove().exec(),
            mongo.Account.remove().exec(),
            mongo.Space.remove().exec()
        ]);
    });

    test('Create new domain', (done) => {
        domainService.batchMerge([testDomains[0]])
            .then((results) => {
                const domain = results.savedDomains[0];

                assert.isNotNull(domain._id);

                return domain._id;
            })
            .then((domainId) => mongo.DomainModel.findById(domainId))
            .then((domain) => {
                assert.isNotNull(domain);
            })
            .then(done)
            .catch(done);
    });

    test('Update existed domain', (done) => {
        const newValType = 'value.Type';

        domainService.batchMerge([testDomains[0]])
            .then((results) => {
                const domain = results.savedDomains[0];

                const domainBeforeMerge = Object.assign({}, testDomains[0], {_id: domain._id, valueType: newValType});

                return domainService.batchMerge([domainBeforeMerge]);
            })
            .then((results) => mongo.DomainModel.findById(results.savedDomains[0]._id))
            .then((domainAfterMerge) => {
                assert.equal(domainAfterMerge.valueType, newValType);
            })
            .then(done)
            .catch(done);
    });

    test('Create duplicated domain', (done) => {
        const dupleDomain = Object.assign({}, testDomains[0], {_id: null});

        domainService.batchMerge([testDomains[0]])
            .then(() => domainService.batchMerge([dupleDomain]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed domain', (done) => {
        domainService.batchMerge([testDomains[0]])
            .then((results) => {
                const domain = results.savedDomains[0];

                return mongo.DomainModel.findById(domain._id)
                    .then((foundDomain) => domainService.remove(foundDomain._id))
                    .then(({rowsAffected}) => {
                        assert.equal(rowsAffected, 1);
                    })
                    .then(() => mongo.DomainModel.findById(domain._id))
                    .then((notFoundDomain) => {
                        assert.isNull(notFoundDomain);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Remove domain without identifier', (done) => {
        domainService.batchMerge([testDomains[0]])
            .then(() => domainService.remove())
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed domain', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        domainService.batchMerge([testDomains[0]])
            .then(() => domainService.remove(validNoExistingId))
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 0);
            })
            .then(done)
            .catch(done);
    });

    test('Remove all domains in space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userDomain = Object.assign({}, testDomains[0], {space: spaces[0][0]._id});

                return domainService.batchMerge([userDomain])
                    .then(() => domainService.removeAll(currentUser._id, false));
            })
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 1);
            })
            .then(done)
            .catch(done);
    });

    test('Get all domains by space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const userDomain = Object.assign({}, testDomains[0], {space: spaces[0][0]._id});

                return domainService.batchMerge([userDomain])
                    .then((results) => {
                        const domain = results.savedDomains[0];

                        return domainService.listBySpaces(spaces[0][0]._id)
                            .then((domains) => {
                                assert.equal(domains.length, 1);
                                assert.equal(domains[0]._id.toString(), domain._id.toString());
                            });
                    });
            })
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
