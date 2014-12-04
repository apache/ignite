/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Simple cache test.
 */
public class GridCachePartitionedBasicOpSelfTest extends GridCacheBasicOpAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setBackups(2);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override public void testBasicOps() throws Exception {
        super.testBasicOps();
    }

    /** {@inheritDoc} */
    @Override public void testBasicOpsAsync() throws Exception {
        super.testBasicOpsAsync();
    }

    /** {@inheritDoc} */
    @Override public void testOptimisticTransaction() throws Exception {
        super.testOptimisticTransaction();
    }

    /** {@inheritDoc} */
    @Override public void testPutWithExpiration() throws Exception {
        super.testPutWithExpiration();
    }
}
