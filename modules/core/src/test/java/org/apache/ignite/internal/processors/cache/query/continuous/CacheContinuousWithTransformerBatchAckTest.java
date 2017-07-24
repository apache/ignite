package org.apache.ignite.internal.processors.cache.query.continuous;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * @author SBT-Izhikov-NV
 */
public class CacheContinuousWithTransformerBatchAckTest extends CacheContinuousBatchAckTest {
    @Override public boolean isContinuousWithTransformer() {
        return true;
    }
}
