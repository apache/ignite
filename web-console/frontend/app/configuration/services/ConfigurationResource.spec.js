
import _ from 'lodash';
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
        const service = configurationResource();
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
