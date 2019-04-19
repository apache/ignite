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

'use strict';

const TestingHelper = require('../TestingHelper');

describe('execute examples >', () => {
    beforeAll((done) => {
        jasmine.DEFAULT_TIMEOUT_INTERVAL = TestingHelper.TIMEOUT;
        done();
    });

    it('CachePutGetExample', (done) => {
        TestingHelper.executeExample('examples/CachePutGetExample.js').
            then(done).
            catch(error => done.fail(error));
    });

    it('SqlExample', (done) => {
        TestingHelper.executeExample('examples/SqlExample.js').
            then(done).
            catch(error => done.fail(error));
    });

    it('SqlQueryEntriesExample', (done) => {
        TestingHelper.executeExample('examples/SqlQueryEntriesExample.js').
            then(done).
            catch(error => done.fail(error));
    });
});
