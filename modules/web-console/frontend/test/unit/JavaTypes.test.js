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

import JavaTypes from '../../app/services/JavaTypes.service.js';

import ClusterDflts from '../../app/modules/configuration/generator/defaults/Cluster.service';
import CacheDflts from '../../app/modules/configuration/generator/defaults/Cache.service';
import IgfsDflts from '../../app/modules/configuration/generator/defaults/IGFS.service';

const INSTANCE = new JavaTypes(new ClusterDflts(), new CacheDflts(), new IgfsDflts());

import { assert } from 'chai';

suite('JavaTypesTestsSuite', () => {
    test('nonBuiltInClass', () => {
        assert.equal(INSTANCE.nonBuiltInClass('BigDecimal'), false);
        assert.equal(INSTANCE.nonBuiltInClass('java.math.BigDecimal'), false);

        assert.equal(INSTANCE.nonBuiltInClass('String'), false);
        assert.equal(INSTANCE.nonBuiltInClass('java.lang.String'), false);

        assert.equal(INSTANCE.nonBuiltInClass('Timestamp'), false);
        assert.equal(INSTANCE.nonBuiltInClass('java.sql.Timestamp'), false);

        assert.equal(INSTANCE.nonBuiltInClass('Date'), false);
        assert.equal(INSTANCE.nonBuiltInClass('java.sql.Date'), false);

        assert.equal(INSTANCE.nonBuiltInClass('Date'), false);
        assert.equal(INSTANCE.nonBuiltInClass('java.util.Date'), false);

        assert.equal(INSTANCE.nonBuiltInClass('CustomClass'), true);
        assert.equal(INSTANCE.nonBuiltInClass('java.util.CustomClass'), true);
        assert.equal(INSTANCE.nonBuiltInClass('my.package.CustomClass'), true);
    });

    test('nonEnum', () => {
        assert.equal(INSTANCE.nonEnum('org.apache.ignite.cache.CacheMode'), false);
        assert.equal(INSTANCE.nonEnum('org.apache.ignite.transactions.TransactionConcurrency'), false);
        assert.equal(INSTANCE.nonEnum('org.apache.ignite.cache.CacheWriteSynchronizationMode'), false);
        assert.equal(INSTANCE.nonEnum('org.apache.ignite.igfs.IgfsIpcEndpointType'), false);
        assert.equal(INSTANCE.nonEnum('java.io.Serializable'), true);
        assert.equal(INSTANCE.nonEnum('BigDecimal'), true);
    });

    test('shortClassName', () => {
        assert.equal(INSTANCE.shortClassName('java.math.BigDecimal'), 'BigDecimal');
        assert.equal(INSTANCE.shortClassName('BigDecimal'), 'BigDecimal');
        assert.equal(INSTANCE.shortClassName('int'), 'int');
        assert.equal(INSTANCE.shortClassName('java.lang.Integer'), 'Integer');
        assert.equal(INSTANCE.shortClassName('Integer'), 'Integer');
        assert.equal(INSTANCE.shortClassName('java.util.UUID'), 'UUID');
        assert.equal(INSTANCE.shortClassName('java.sql.Date'), 'Date');
        assert.equal(INSTANCE.shortClassName('Date'), 'Date');
        assert.equal(INSTANCE.shortClassName('com.my.Abstract'), 'Abstract');
        assert.equal(INSTANCE.shortClassName('Abstract'), 'Abstract');
    });

    test('fullClassName', () => {
        assert.equal(INSTANCE.fullClassName('BigDecimal'), 'java.math.BigDecimal');
    });

    test('validIdentifier', () => {
        assert.equal(INSTANCE.validIdentifier('myIdent'), true);
        assert.equal(INSTANCE.validIdentifier('java.math.BigDecimal'), false);
        assert.equal(INSTANCE.validIdentifier('2Demo'), false);
        assert.equal(INSTANCE.validIdentifier('abra kadabra'), false);
        assert.equal(INSTANCE.validIdentifier(undefined), false);
        assert.equal(INSTANCE.validIdentifier(null), false);
        assert.equal(INSTANCE.validIdentifier(''), false);
        assert.equal(INSTANCE.validIdentifier(' '), false);
    });

    test('validClassName', () => {
        assert.equal(INSTANCE.validClassName('java.math.BigDecimal'), true);
        assert.equal(INSTANCE.validClassName('2Demo'), false);
        assert.equal(INSTANCE.validClassName('abra kadabra'), false);
        assert.equal(INSTANCE.validClassName(undefined), false);
        assert.equal(INSTANCE.validClassName(null), false);
        assert.equal(INSTANCE.validClassName(''), false);
        assert.equal(INSTANCE.validClassName(' '), false);
    });

    test('validPackage', () => {
        assert.equal(INSTANCE.validPackage('java.math.BigDecimal'), true);
        assert.equal(INSTANCE.validPackage('my.org.SomeClass'), true);
        assert.equal(INSTANCE.validPackage('25'), false);
        assert.equal(INSTANCE.validPackage('abra kadabra'), false);
        assert.equal(INSTANCE.validPackage(''), false);
        assert.equal(INSTANCE.validPackage(' '), false);
    });

    test('packageSpecified', () => {
        assert.equal(INSTANCE.packageSpecified('java.math.BigDecimal'), true);
        assert.equal(INSTANCE.packageSpecified('BigDecimal'), false);
    });

    test('isKeyword', () => {
        assert.equal(INSTANCE.isKeyword('abstract'), true);
        assert.equal(INSTANCE.isKeyword('Abstract'), true);
        assert.equal(INSTANCE.isKeyword('abra kadabra'), false);
        assert.equal(INSTANCE.isKeyword(undefined), false);
        assert.equal(INSTANCE.isKeyword(null), false);
        assert.equal(INSTANCE.isKeyword(''), false);
        assert.equal(INSTANCE.isKeyword(' '), false);
    });

    test('isPrimitive', () => {
        assert.equal(INSTANCE.isPrimitive('boolean'), true);
    });

    test('validUUID', () => {
        assert.equal(INSTANCE.validUUID('123e4567-e89b-12d3-a456-426655440000'), true);
        assert.equal(INSTANCE.validUUID('12345'), false);
        assert.equal(INSTANCE.validUUID(undefined), false);
        assert.equal(INSTANCE.validUUID(null), false);
        assert.equal(INSTANCE.validUUID(''), false);
        assert.equal(INSTANCE.validUUID(' '), false);
    });
});
