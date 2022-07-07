package org.apache.ignite.internal.ducktest.tests.self_test;

import org.apache.ignite.cache.affinity.AffinityKeyMapper;

/**
 * Affinity mapper used to test configuration of the custom affinity mapper in the server node config XML.
 */
public class TestAffinityMapper implements AffinityKeyMapper {
    /** {@inheritDoc} */
    @Override public Object affinityKey(Object key) {
        return key;
    }

    /** {@inheritDoc} */
    @Override public void reset() {
    }
}
