/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {assert} from 'chai';
import {IgniteObjectDiffer} from './ConfigChangesGuard';

suite('Config changes guard', () => {
    test('Object differ', () => {
        const differ = new IgniteObjectDiffer();

        assert.isUndefined(
            differ.diff({a: void 0}, {a: false}),
            'No changes when boolean values changes from undefined to false'
        );

        assert.isUndefined(
            differ.diff({a: void 0}, {a: null}),
            'No changes when undefined value changes to null'
        );

        assert.isUndefined(
            differ.diff({a: void 0}, {a: ''}),
            'No changes when undefined value changes to an empty string'
        );
    });
});
