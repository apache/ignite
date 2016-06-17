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

describe('JavaTypes Service Test', () => {
    describe('nonBuiltInClass', () => {
        it('BigDecimal', () => {
            assert.equal(nonBuiltInClass('BigDecimal'), false);
        });
    });

    describe('fullClassName', () => {
        it('java.math.BigDecimal', () => {
            assert.equal(fullClassName('BigDecimal'), 'java.math.BigDecimal');
        });
    });

    describe('validIdentifier', () => {
        it('java.math.BigDecimal', () => {
            assert.equal(validIdentifier('java.math.BigDecimal'), true);
        });
    });

    describe('validPackage', () => {
        it('java.math.BigDecimal.', () => {
            assert.equal(validPackage('java.math.BigDecimal'), true);
        });
    });

    describe('packageSpecified', () => {
        it('java.math.BigDecimal.', () => {
            assert.equal(packageSpecified('java.math.BigDecimal'), true);
        });
    });

    describe('isKeywords', () => {
        it('java.math.BigDecimal.', () => {
            assert.equal(isKeywords('abstract'), true);
        });
    });

    describe('isJavaPrimitive', () => {
        it('boolean', () => {
            assert.equal(isJavaPrimitive('boolean'), true);
        });
    });
});
