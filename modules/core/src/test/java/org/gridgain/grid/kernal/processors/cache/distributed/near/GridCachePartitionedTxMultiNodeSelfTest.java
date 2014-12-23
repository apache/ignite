/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test basic cache operations in transactions.
 */
public class GridCachePartitionedTxMultiNodeSelfTest extends IgniteTxMultiNodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        // Default cache configuration.
        GridCacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public void testPutOneEntryInTx() throws Exception {
        super.testPutOneEntryInTx();
    }

    /** {@inheritDoc} */
    @Override public void testPutOneEntryInTxMultiThreaded() throws Exception {
        super.testPutOneEntryInTxMultiThreaded();
    }

    /** {@inheritDoc} */
    @Override public void testPutTwoEntriesInTx() throws Exception {
        super.testPutTwoEntriesInTx();
    }

    /** {@inheritDoc} */
    @Override public void testPutTwoEntryInTxMultiThreaded() throws Exception {
        super.testPutTwoEntryInTxMultiThreaded();
    }

    /** {@inheritDoc} */
    @Override public void testRemoveInTxQueried() throws Exception {
        super.testRemoveInTxQueried();
    }

    /** {@inheritDoc} */
    @Override public void testRemoveInTxQueriedMultiThreaded() throws Exception {
        super.testRemoveInTxQueriedMultiThreaded();
    }

    /** {@inheritDoc} */
    @Override public void testRemoveInTxSimple() throws Exception {
        super.testRemoveInTxSimple();
    }
}
