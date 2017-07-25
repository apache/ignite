package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 */
public class CacheContinuousWithTransformerBatchAckTest extends CacheContinuousBatchAckTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
