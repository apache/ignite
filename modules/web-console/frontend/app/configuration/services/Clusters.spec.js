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

import {suite, test} from 'mocha';
import {assert} from 'chai';
import {spy} from 'sinon';

import Provider from './Clusters';

const mocks = () => new Map([
    ['$http', {
        post: spy()
    }]
]);

suite('Clusters service', () => {
    test('discoveries', () => {
        const s = new Provider(...mocks().values());
        assert.isArray(s.discoveries, 'has discoveries array');
        assert.isOk(s.discoveries.every((d) => d.value && d.label), 'discoveries have correct format');
    });

    test('minMemoryPolicySize', () => {
        const s = new Provider(...mocks().values());
        assert.isNumber(s.minMemoryPolicySize, 'has minMemoryPolicySize number');
    });

    test('saveCluster', () => {
        const s = new Provider(...mocks().values());
        const cluster = {id: 1, name: 'Test'};
        s.saveCluster(cluster);
        assert.isOk(s.$http.post.called, 'calls $http.post');
        assert.equal(s.$http.post.lastCall.args[0], '/api/v1/configuration/clusters/save', 'uses correct API URL');
        assert.deepEqual(s.$http.post.lastCall.args[1], cluster, 'sends cluster');
    });

    test('getBlankCluster', () => {
        const s = new Provider(...mocks().values());
        assert.isObject(s.getBlankCluster());
    });
});
