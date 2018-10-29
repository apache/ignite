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

const instance = defaultName();

suite('defaultName', () => {
    test('defaultName filter', () => {
        let undef;

        assert.equal(instance(''), '<default>');
        assert.equal(instance(null), '<default>');
        assert.equal(instance(), '<default>');
        assert.equal(instance('', false), '<default>');
        assert.equal(instance(null, false), '<default>');
        assert.equal(instance(undef, false), '<default>');
        assert.equal(instance('', true), '&lt;default&gt;');
        assert.equal(instance(null, true), '&lt;default&gt;');
        assert.equal(instance(undef, true), '&lt;default&gt;');
        assert.equal(instance('name', false), 'name');
        assert.equal(instance('name', true), 'name');
    });
});
