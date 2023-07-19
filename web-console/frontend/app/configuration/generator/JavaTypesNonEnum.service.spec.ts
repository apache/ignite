

import {JavaTypesNonEnum} from './JavaTypesNonEnum.service';

import ClusterDflts from './generator/defaults/Cluster.service';
import CacheDflts from './generator/defaults/Cache.service';
import JavaTypes from 'app/services/JavaTypes.service';

const instance = new JavaTypesNonEnum(new ClusterDflts(), new CacheDflts(), new JavaTypes());

import { assert } from 'chai';

suite('JavaTypesNonEnum', () => {
    test('nonEnum', () => {
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheMode'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.transactions.TransactionConcurrency'), false);
        assert.equal(instance.nonEnum('org.apache.ignite.cache.CacheWriteSynchronizationMode'), false);
        assert.equal(instance.nonEnum('java.io.Serializable'), true);
        assert.equal(instance.nonEnum('BigDecimal'), true);
    });
});
