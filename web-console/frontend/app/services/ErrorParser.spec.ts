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

import ErrorParser from './ErrorParser.service';
import JavaTypes from './JavaTypes.service';

const parser = new ErrorParser(new JavaTypes());

import { assert } from 'chai';

const EXPECTED_CAUSES = ['Root cause', 'Duplicate cause', 'Final cause'];

const TEST_ERROR = {
    className: 'test.Exception1',
    message: `Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause, trace=...]
    at test.Class1.function(Class1.java:1)
    ... 9 more 
Caused by: class test.Exception2: Final cause
    at test.Class2.function(Class2.java:1)
    ... 14 more
Caused by: class test.Exception3: Duplicate cause
    at test.Class3.function(Class3.java:1)
    ... 14 more
Caused by: class test.Exception4: Duplicate cause
    at test.Class4.function(Class4.java:1)
    ... 29 more
Caused by: class test.Exception5: Root cause
    at test.Class5.function(Class5.java:1)
    ... 40 more
]`
};

const FULL_ERROR_MSG = '[Exception1] Failed to handle request: [req=EXE, taskName=test.TaskName, params=[], err=Final cause';
const ERROR_MSG = 'Test: Final cause';

suite('Error parser service', () => {
    test('Error parsing', () => {
        assert.equal(FULL_ERROR_MSG, parser.extractFullMessage(TEST_ERROR));

        const parsed = parser.parse(TEST_ERROR, 'Test: ');

        assert.equal(ERROR_MSG, parsed.message);

        assert.deepEqual(EXPECTED_CAUSES, parsed.causes);
    });
});
