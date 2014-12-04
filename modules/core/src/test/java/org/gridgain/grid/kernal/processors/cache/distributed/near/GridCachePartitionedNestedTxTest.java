/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.IgniteConfiguration;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;


import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Partitioned cache nested transaction test.
 */
public class GridCachePartitionedNestedTxTest extends GridCacheNestedTxAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);

        // Explicitly set number of backups equal to number of grids.
        cacheCfg.setBackups(1);

        // Query should be executed without ongoing transactions.
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }
}
