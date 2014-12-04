/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures.partitioned;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache sequence basic tests.
 */
public class GridCachePartitionedSequenceApiSelfTest extends GridCacheSequenceApiSelfAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        // Default cache configuration.
        GridCacheConfiguration dfltCacheCfg = defaultCacheConfiguration();

        dfltCacheCfg.setCacheMode(PARTITIONED);
        dfltCacheCfg.setBackups(1);
        dfltCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dfltCacheCfg.setAtomicSequenceReserveSize(BATCH_SIZE);
        dfltCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dfltCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(dfltCacheCfg);

        return cfg;
    }
}
