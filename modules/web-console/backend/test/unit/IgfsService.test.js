/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

const assert = require('chai').assert;
const injector = require('../injector');
const testIgfss = require('../data/igfss.json');
const testAccounts = require('../data/accounts.json');
const testSpaces = require('../data/spaces.json');

let igfsService;
let mongo;
let errors;
let db;

suite('IgfsServiceTestsSuite', () => {
    suiteSetup(() => {
        return Promise.all([injector('services/igfss'),
            injector('mongo'),
            injector('errors'),
            injector('dbHelper')])
            .then(([_igfsService, _mongo, _errors, _db]) => {
                mongo = _mongo;
                igfsService = _igfsService;
                errors = _errors;
                db = _db;
            });
    });

    setup(() => db.init());

    test('Create new igfs', (done) => {
        const dupleIgfs = Object.assign({}, testIgfss[0], {name: 'Other name'});

        delete dupleIgfs._id;

        igfsService.merge(dupleIgfs)
            .then((igfs) => {
                assert.isNotNull(igfs._id);

                return mongo.Igfs.findById(igfs._id);
            })
            .then((igfs) =>
                assert.isNotNull(igfs)
            )
            .then(done)
            .catch(done);
    });

    test('Update existed igfs', (done) => {
        const newName = 'NewUniqueName';

        const igfsBeforeMerge = Object.assign({}, testIgfss[0], {name: newName});

        igfsService.merge(igfsBeforeMerge)
            .then((igfs) => mongo.Igfs.findById(igfs._id))
            .then((igfsAfterMerge) => assert.equal(igfsAfterMerge.name, newName))
            .then(done)
            .catch(done);
    });

    test('Create duplicated igfs', (done) => {
        const dupleIfgs = Object.assign({}, testIgfss[0]);

        delete dupleIfgs._id;

        igfsService.merge(dupleIfgs)
            .catch((err) => {
                assert.instanceOf(err, errors.DuplicateKeyException);

                done();
            });
    });

    test('Remove existed igfs', (done) => {
        igfsService.remove(testIgfss[0]._id)
            .then(({rowsAffected}) => assert.equal(rowsAffected, 1))
            .then(() => mongo.Igfs.findById(testIgfss[0]._id))
            .then((notFoundIgfs) =>
                assert.isNull(notFoundIgfs)
            )
            .then(done)
            .catch(done);
    });

    test('Remove igfs without identifier', (done) => {
        igfsService.remove()
            .catch((err) => {
                assert.instanceOf(err, errors.IllegalArgumentException);

                done();
            });
    });

    test('Remove missed igfs', (done) => {
        const validNoExistingId = 'FFFFFFFFFFFFFFFFFFFFFFFF';

        igfsService.remove(validNoExistingId)
            .then(({rowsAffected}) => assert.equal(rowsAffected, 0))
            .then(done)
            .catch(done);
    });

    test('Get all igfss by space', (done) => {
        igfsService.listBySpaces(testSpaces[0]._id)
            .then((igfss) => assert.equal(igfss.length, 1))
            .then(done)
            .catch(done);
    });

    test('Remove all igfss in space', (done) => {
        igfsService.removeAll(testAccounts[0]._id, false)
            .then(({rowsAffected}) => assert.equal(rowsAffected, 1))
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
