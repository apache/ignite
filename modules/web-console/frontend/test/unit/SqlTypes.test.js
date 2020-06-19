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
