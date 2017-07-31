package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 */
public class CacheContinuousQueryWithTransformerAsyncFilterListenerTest extends CacheContinuousQueryAsyncFilterListenerTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
