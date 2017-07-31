package org.apache.ignite.internal.processors.cache.query.continuous;

/**
 */
public class CacheContinuousQueryWithTransformerExecuteInPrimaryTest extends CacheContinuousQueryExecuteInPrimaryTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
