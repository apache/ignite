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

import configurationResource from './ConfigurationResource';

import { suite, test } from 'mocha';
import { assert } from 'chai';

const CHECKED_CONFIGURATION = {
    spaces: [{
        id: '1space',
        name: 'Test space'
    }],
    clusters: [{
        id: '1cluster',
        space: '1space',
        name: 'Test cluster',
        caches: ['1cache'],
        models: ['1model']
    }],
    caches: [{
        id: '1cache',
        space: '1space',
        name: 'Test cache',
        clusters: ['1cluster'],
        models: ['1model']
    }],
    domains: [{
        id: '1model',
        space: '1space',
        name: 'Test model',
        clusters: ['1cluster'],
        caches: ['1cache']
    }]
};

suite('ConfigurationResourceTestsSuite', () => {
    test('ConfigurationResourceService correctly populate data', async() => {
        const service = configurationResource(null);
        const converted = _.cloneDeep(CHECKED_CONFIGURATION);
        const res = await service.populate(converted);

        assert.notEqual(res.clusters[0], converted.clusters[0]);

        assert.deepEqual(converted.clusters[0].caches, CHECKED_CONFIGURATION.clusters[0].caches);
        assert.deepEqual(converted.clusters[0].models, CHECKED_CONFIGURATION.clusters[0].models);

        assert.deepEqual(converted.caches[0].clusters, CHECKED_CONFIGURATION.caches[0].clusters);
        assert.deepEqual(converted.caches[0].models, CHECKED_CONFIGURATION.caches[0].models);

        assert.deepEqual(converted.domains[0].clusters, CHECKED_CONFIGURATION.domains[0].clusters);
        assert.deepEqual(converted.domains[0].caches, CHECKED_CONFIGURATION.domains[0].caches);
    });
});
