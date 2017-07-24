package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 * @author SBT-Izhikov-NV
 */
public class CacheContinuousQueryWithTransformerAsyncFilterListenerTest extends CacheContinuousQueryAsyncFilterListenerTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
