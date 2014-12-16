/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test cases for multi-threaded tests.
 */
public class GridCachePartitionedMultiNodeLockSelfTest extends GridCacheMultiNodeLockAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(2); // 2 backups, so all nodes are involved.
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected boolean partitioned() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void testBasicLock() throws Exception {
        super.testBasicLock();
    }

    /** {@inheritDoc} */
    @Override public void testLockMultithreaded() throws Exception {
        super.testLockMultithreaded();
    }

    /** {@inheritDoc} */
    @Override public void testLockReentry() throws IgniteCheckedException {
        super.testLockReentry();
    }

    /** {@inheritDoc} */
    @Override public void testMultiNodeLock() throws Exception {
        super.testMultiNodeLock();
    }

    /** {@inheritDoc} */
    @Override public void testMultiNodeLockAsync() throws Exception {
        super.testMultiNodeLockAsync();
    }

    /** {@inheritDoc} */
    @Override public void testMultiNodeLockAsyncWithKeyLists() throws Exception {
        super.testMultiNodeLockAsyncWithKeyLists();
    }

    /** {@inheritDoc} */
    @Override public void testMultiNodeLockWithKeyLists() throws Exception {
        super.testMultiNodeLockWithKeyLists();
    }
}
