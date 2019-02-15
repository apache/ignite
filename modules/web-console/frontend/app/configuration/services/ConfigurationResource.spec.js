/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

import configurationResource from './ConfigurationResource';

import { suite, test } from 'mocha';
import { assert } from 'chai';

const CHECKED_CONFIGURATION = {
    spaces: [{
        _id: '1space',
        name: 'Test space'
    }],
    clusters: [{
        _id: '1cluster',
        space: '1space',
        name: 'Test cluster',
        caches: ['1cache'],
        models: ['1model'],
        igfss: ['1igfs']
    }],
    caches: [{
        _id: '1cache',
        space: '1space',
        name: 'Test cache',
        clusters: ['1cluster'],
        models: ['1model']
    }],
    domains: [{
        _id: '1model',
        space: '1space',
        name: 'Test model',
        clusters: ['1cluster'],
        caches: ['1cache']
    }],
    igfss: [{
        _id: '1igfs',
        space: '1space',
        name: 'Test IGFS',
        clusters: ['1cluster']
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
        assert.deepEqual(converted.clusters[0].igfss, CHECKED_CONFIGURATION.clusters[0].igfss);

        assert.deepEqual(converted.caches[0].clusters, CHECKED_CONFIGURATION.caches[0].clusters);
        assert.deepEqual(converted.caches[0].models, CHECKED_CONFIGURATION.caches[0].models);

        assert.deepEqual(converted.domains[0].clusters, CHECKED_CONFIGURATION.domains[0].clusters);
        assert.deepEqual(converted.domains[0].caches, CHECKED_CONFIGURATION.domains[0].caches);

        assert.deepEqual(converted.igfss[0].clusters, CHECKED_CONFIGURATION.igfss[0].clusters);
    });
});
