package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 */
public class CacheContinuousQueryWithTransformerConcurrentPartitionUpdateTest
    extends CacheContinuousQueryConcurrentPartitionUpdateTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
