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

import JavaTypes from '../../app/services/JavaTypes.service';

import ClusterDflts from '../../app/modules/configuration/generator/defaults/Cluster.service';
import CacheDflts from '../../app/modules/configuration/generator/defaults/Cache.service';
import IgfsDflts from '../../app/modules/configuration/generator/defaults/IGFS.service';

const instance = new JavaTypes(new ClusterDflts(), new CacheDflts(), new IgfsDflts());

import { suite, test } from 'mocha';
import { assert } from 'chai';

suite('JavaTypesTestsSuite', () => {
    test('nonBuiltInClass', () => {
        assert.equal(instance.nonBuiltInClass('BigDecimal'), false);
        assert.equal(instance.nonBuiltInClass('java.math.BigDecimal'), false);

        assert.equal(instance.nonBuiltInClass('String'), false);
        assert.equal(instance.nonBuiltInClass('java.lang.String'), false);

        assert.equal(instance.nonBuiltInClass('Timestamp'), false);
        assert.equal(instance.nonBuiltInClass('java.sql.Timestamp'), false);

        assert.equal(instance.nonBuiltInClass('Date'), false);
        assert.equal(instance.nonBuiltInClass('java.sql.Date'), false);

        assert.equal(instance.nonBuiltInClass('Date'), false);
        assert.equal(instance.nonBuiltInClass('java.util.Date'), false);

        assert.equal(instance.nonBuiltInClass('CustomClass'), true);
        assert.equal(instance.nonBuiltInClass('java.util.CustomClass'), true);
        assert.equal(instance.nonBuiltInClass('my.package.CustomClass'), true);
    });

    test('nonEnum', () => {
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheMode'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.transactions.TransactionConcurrency'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheWriteSynchronizationMode'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.igfs.IgfsIpcEndpointType'), false);
        assert.equal(instance.nonEnum('java.io.Serializable'), true);
        assert.equal(instance.nonEnum('BigDecimal'), true);
    });

    test('shortClassName', () => {
        assert.equal(instance.shortClassName('java.math.BigDecimal'), 'BigDecimal');
        assert.equal(instance.shortClassName('BigDecimal'), 'BigDecimal');
        assert.equal(instance.shortClassName('int'), 'int');
        assert.equal(instance.shortClassName('java.lang.Integer'), 'Integer');
        assert.equal(instance.shortClassName('Integer'), 'Integer');
        assert.equal(instance.shortClassName('java.util.UUID'), 'UUID');
        assert.equal(instance.shortClassName('java.sql.Date'), 'Date');
        assert.equal(instance.shortClassName('Date'), 'Date');
        assert.equal(instance.shortClassName('com.my.Abstract'), 'Abstract');
        assert.equal(instance.shortClassName('Abstract'), 'Abstract');
    });

    test('fullClassName', () => {
        assert.equal(instance.fullClassName('BigDecimal'), 'java.math.BigDecimal');
    });

    test('validIdentifier', () => {
        assert.equal(instance.validIdentifier('myIdent'), true);
        assert.equal(instance.validIdentifier('java.math.BigDecimal'), false);
        assert.equal(instance.validIdentifier('2Demo'), false);
        assert.equal(instance.validIdentifier('abra kadabra'), false);
        assert.equal(instance.validIdentifier(), false);
        assert.equal(instance.validIdentifier(null), false);
        assert.equal(instance.validIdentifier(''), false);
        assert.equal(instance.validIdentifier(' '), false);
    });

    test('validClassName', () => {
        assert.equal(instance.validClassName('java.math.BigDecimal'), true);
        assert.equal(instance.validClassName('2Demo'), false);
        assert.equal(instance.validClassName('abra kadabra'), false);
        assert.equal(instance.validClassName(), false);
        assert.equal(instance.validClassName(null), false);
        assert.equal(instance.validClassName(''), false);
        assert.equal(instance.validClassName(' '), false);
    });

    test('validPackage', () => {
        assert.equal(instance.validPackage('java.math.BigDecimal'), true);
        assert.equal(instance.validPackage('my.org.SomeClass'), true);
        assert.equal(instance.validPackage('25'), false);
        assert.equal(instance.validPackage('abra kadabra'), false);
        assert.equal(instance.validPackage(''), false);
        assert.equal(instance.validPackage(' '), false);
    });

    test('packageSpecified', () => {
        assert.equal(instance.packageSpecified('java.math.BigDecimal'), true);
        assert.equal(instance.packageSpecified('BigDecimal'), false);
    });

    test('isKeyword', () => {
        assert.equal(instance.isKeyword('abstract'), true);
        assert.equal(instance.isKeyword('Abstract'), true);
        assert.equal(instance.isKeyword('abra kadabra'), false);
        assert.equal(instance.isKeyword(), false);
        assert.equal(instance.isKeyword(null), false);
        assert.equal(instance.isKeyword(''), false);
        assert.equal(instance.isKeyword(' '), false);
    });

    test('isPrimitive', () => {
        assert.equal(instance.isPrimitive('boolean'), true);
    });

    test('validUUID', () => {
        assert.equal(instance.validUUID('123e4567-e89b-12d3-a456-426655440000'), true);
        assert.equal(instance.validUUID('12345'), false);
        assert.equal(instance.validUUID(), false);
        assert.equal(instance.validUUID(null), false);
        assert.equal(instance.validUUID(''), false);
        assert.equal(instance.validUUID(' '), false);
    });
});
