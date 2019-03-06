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

import SqlTypes from '../../app/services/SqlTypes.service';

const INSTANCE = new SqlTypes();

import { suite, test } from 'mocha';
import { assert } from 'chai';

suite('SqlTypesTestsSuite', () => {
    test('validIdentifier', () => {
        assert.equal(INSTANCE.validIdentifier('myIdent'), true);
        assert.equal(INSTANCE.validIdentifier('java.math.BigDecimal'), false);
        assert.equal(INSTANCE.validIdentifier('2Demo'), false);
        assert.equal(INSTANCE.validIdentifier('abra kadabra'), false);
        assert.equal(INSTANCE.validIdentifier(), false);
        assert.equal(INSTANCE.validIdentifier(null), false);
        assert.equal(INSTANCE.validIdentifier(''), false);
        assert.equal(INSTANCE.validIdentifier(' '), false);
    });

    test('isKeyword', () => {
        assert.equal(INSTANCE.isKeyword('group'), true);
        assert.equal(INSTANCE.isKeyword('Group'), true);
        assert.equal(INSTANCE.isKeyword('select'), true);
        assert.equal(INSTANCE.isKeyword('abra kadabra'), false);
        assert.equal(INSTANCE.isKeyword(), false);
        assert.equal(INSTANCE.isKeyword(null), false);
        assert.equal(INSTANCE.isKeyword(''), false);
        assert.equal(INSTANCE.isKeyword(' '), false);
    });

    test('findJdbcType', () => {
        assert.equal(INSTANCE.findJdbcType(0).dbName, 'NULL');
        assert.equal(INSTANCE.findJdbcType(5555).dbName, 'Unknown');
    });
});
