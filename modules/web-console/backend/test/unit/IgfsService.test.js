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


import {assert} from 'chai';
import injector from '../injector';
import testIgfss from '../data/igfss.json';
import testAccounts from '../data/accounts.json';

let igfsService;
let mongo;
let errors;

suite('IgfsServiceTestsSuite', () => {
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
        return Promise.all([injector('services/igfss'),
            injector('mongo'),
            injector('errors')])
            .then(([_igfsService, _mongo, _errors]) => {
                mongo = _mongo;
                igfsService = _igfsService;
                errors = _errors;
            });
    });

    setup(() => {
        return Promise.all([
            mongo.Igfs.remove().exec(),
            mongo.Account.remove().exec(),
            mongo.Space.remove().exec()
        ]);
    });

    test('Create new igfs', (done) => {
        igfsService.merge(testIgfss[0])
            .then((igfs) => {
                assert.isNotNull(igfs._id);

                return igfs._id;
            })
            .then((igfsId) => mongo.Igfs.findById(igfsId))
            .then((igfs) => {
                assert.isNotNull(igfs);
            })
            .then(done)
            .catch(done);
    });

    test('Update existed igfs', (done) => {
        const newName = 'NewUniqueName';

        igfsService.merge(testIgfss[0])
            .then((existIgfs) => {
                const igfsBeforeMerge = {...testIgfss[0], _id: existIgfs._id, name: newName};

                return igfsService.merge(igfsBeforeMerge);
            })
            .then((igfs) => mongo.Igfs.findById(igfs._id))
            .then((igfsAfterMerge) => {
                assert.equal(igfsAfterMerge.name, newName);
            })
            .then(done)
            .catch(done);
    });

    test('Create duplicated igfs', (done) => {
        igfsService.merge(testIgfss[0])
            .then(() => igfsService.merge(testIgfss[0]))
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed igfs', (done) => {
        igfsService.merge(testIgfss[0])
            .then((existIgfs) => {
                return mongo.Igfs.findById(existIgfs._id)
                    .then((foundIgfs) => igfsService.remove(foundIgfs._id))
                    .then(({rowsAffected}) => {
                        assert.equal(rowsAffected, 1);
                    })
                    .then(() => mongo.Igfs.findById(existIgfs._id))
                    .then((notFoundIgfs) => {
                        assert.isNull(notFoundIgfs);
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Remove igfs without identifier', (done) => {
        igfsService.merge(testIgfss[0])
            .then(() => igfsService.remove())
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed igfs', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        igfsService.merge(testIgfss[0])
            .then(() => igfsService.remove(validNoExistingId))
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 0);
            })
            .then(done)
            .catch(done);
    });

    test('Remove all igfss in space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const currentUser = accounts[0];
                const userIgfs = {...testIgfss[0], space: spaces[0][0]._id};

                return igfsService.merge(userIgfs)
                    .then(() => igfsService.removeAll(currentUser._id, false));
            })
            .then(({rowsAffected}) => {
                assert.equal(rowsAffected, 1);
            })
            .then(done)
            .catch(done);
    });

    test('Get all igfss by space', (done) => {
        prepareUserSpaces()
            .then(([accounts, spaces]) => {
                const userIgfs = {...testIgfss[0], space: spaces[0][0]._id};

                return igfsService.merge(userIgfs)
                    .then((existIgfs) => {
                        return igfsService.listBySpaces(spaces[0][0]._id)
                            .then((igfss) => {
                                assert.equal(igfss.length, 1);
                                assert.equal(igfss[0]._id.toString(), existIgfs._id.toString());
                            });
                    });
            })
            .then(done)
            .catch(done);
    });

    test('Update linked entities on update igfs', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove igfs', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });

    test('Update linked entities on remove all igfss in space', (done) => {
        // TODO IGNITE-3262 Add test.
        done();
    });
});
