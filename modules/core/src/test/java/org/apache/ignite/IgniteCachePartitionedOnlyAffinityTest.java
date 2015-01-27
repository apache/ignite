package org.apache.ignite;

import org.apache.ignite.cache.*;

/**
 * Tests for {@link org.apache.ignite.internal.processors.affinity.GridAffinityProcessor.CacheAffinityProxy}.
 */
public class IgniteCachePartitionedOnlyAffinityTest extends IgniteCacheAffinityAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override  protected CacheDistributionMode distributionMode() {
        return CacheDistributionMode.PARTITIONED_ONLY;
    }
}
