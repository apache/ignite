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

import { assert } from 'chai';

const { nonBuiltInClass, fullClassName, validIdentifier, validPackage, packageSpecified, isKeywords, isJavaPrimitive} = JavaTypes[1]();

suite('JavaTypesTestsSuite', () => {
    test('nonBuiltInClass', () => {
        assert.equal(nonBuiltInClass('BigDecimal'), false);
        assert.equal(nonBuiltInClass('java.math.BigDecimal'), false);

        assert.equal(nonBuiltInClass('String'), false);
        assert.equal(nonBuiltInClass('java.lang.String'), false);

        assert.equal(nonBuiltInClass('Timestamp'), false);
        assert.equal(nonBuiltInClass('java.sql.Timestamp'), false);

        assert.equal(nonBuiltInClass('Date'), false);
        assert.equal(nonBuiltInClass('java.sql.Date'), false);

        assert.equal(nonBuiltInClass('Date'), false);
        assert.equal(nonBuiltInClass('java.util.Date'), false);

        assert.equal(nonBuiltInClass('CustomClass'), true);
        assert.equal(nonBuiltInClass('java.util.CustomClass'), true);
        assert.equal(nonBuiltInClass('my.package.CustomClass'), true);
    });

    test('fullClassName', () => {
        assert.equal(fullClassName('BigDecimal'), 'java.math.BigDecimal');
    });

    test('validIdentifier', () => {
        assert.equal(validIdentifier('java.math.BigDecimal'), true);
    });

    test('validPackage', () => {
        assert.equal(validPackage('java.math.BigDecimal'), true);
    });

    test('packageSpecified', () => {
        assert.equal(packageSpecified('java.math.BigDecimal'), true);
    });

    test('isKeywords', () => {
        assert.equal(isKeywords('abstract'), true);
    });

    test('isJavaPrimitive', () => {
        assert.equal(isJavaPrimitive('boolean'), true);
    });
});
