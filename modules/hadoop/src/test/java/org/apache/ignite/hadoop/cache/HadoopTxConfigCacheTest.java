package org.apache.ignite.hadoop.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.IgniteTxConfigCacheSelfTest;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 * Test checks whether hadoop system cache doesn't use user defined TX config.
 */
public class HadoopTxConfigCacheTest  extends IgniteTxConfigCacheSelfTest {
    /**
     * Success if system caches weren't timed out.
     *
     * @throws Exception
     */
    public void testSystemCacheTx() throws Exception {
        final Ignite ignite = grid(0);

        final IgniteInternalCache<Object, Object> hadoopCache = getSystemCache(ignite, CU.SYS_CACHE_HADOOP_MR);

        checkImplicitTxSuccess(hadoopCache);
        checkStartTxSuccess(hadoopCache);
    }
}