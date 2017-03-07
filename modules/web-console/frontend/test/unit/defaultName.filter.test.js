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

import defaultName from '../../app/filters/default-name.filter';

import { assert } from 'chai';

const INSTANCE = defaultName[0]();

suite('defaultName', () => {
    test('defaultName filter', () => {
        assert.equal(INSTANCE(''), '<default>');
        assert.equal(INSTANCE(null), '<default>');
        assert.equal(INSTANCE(undefined), '<default>');
        assert.equal(INSTANCE('', false), '<default>');
        assert.equal(INSTANCE(null, false), '<default>');
        assert.equal(INSTANCE(undefined, false), '<default>');
        assert.equal(INSTANCE('', true), '&lt;default&gt;');
        assert.equal(INSTANCE(null, true), '&lt;default&gt;');
        assert.equal(INSTANCE(undefined, true), '&lt;default&gt;');
        assert.equal(INSTANCE("name", false), 'name');
        assert.equal(INSTANCE("name", true), 'name');
    });
});
