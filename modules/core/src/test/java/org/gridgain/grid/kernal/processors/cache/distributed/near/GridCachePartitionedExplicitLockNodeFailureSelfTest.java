/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests for node failure in transactions.
 */
public class GridCachePartitionedExplicitLockNodeFailureSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    public static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setTxSerializableEnabled(true);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setDgcFrequency(0);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setBackups(GRID_CNT - 1);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** @throws Exception If check failed. */
    @SuppressWarnings("ErrorNotRethrown")
    public void testLockFromNearOrBackup() throws Exception {
        startGrids(GRID_CNT);

        int idx = 0;

        info("Grid will be stopped: " + idx);

        Integer key = 0;

        while (grid(idx).mapKeyToNode(null, key).id().equals(grid(0).localNode().id()))
            key++;

        ClusterNode node = grid(idx).mapKeyToNode(null, key);

        info("Primary node for key [id=" + node.id() + ", order=" + node.order() + ", key=" + key + ']');

        GridCache<Integer, String> cache = cache(idx);

        cache.put(key, "val");

        assert cache.lock(key, -1);

        for (int checkIdx = 1; checkIdx < GRID_CNT; checkIdx++) {
            info("Check grid index: " + checkIdx);

            GridCache<Integer, String> checkCache = cache(checkIdx);

            assert !checkCache.lock(key, -1);

            GridCacheEntry e = checkCache.entry(key);

            assert e.isLocked() : "Entry is not locked for grid [idx=" + checkIdx + ", entry=" + e + ']';
        }

        Collection<IgniteFuture<?>> futs = new LinkedList<>();

        for (int i = 1; i < GRID_CNT; i++) {
            futs.add(
                waitForLocalEvent(grid(i).events(), new P1<GridEvent>() {
                    @Override public boolean apply(GridEvent e) {
                        info("Received grid event: " + e);

                        return true;
                    }
                }, EVT_NODE_LEFT));
        }

        stopGrid(idx);

        for (IgniteFuture<?> fut : futs)
            fut.get();

        for (int i = 0; i < 3; i++) {
            try {
                for (int checkIdx = 1; checkIdx < GRID_CNT; checkIdx++) {
                    info("Check grid index: " + checkIdx);

                    GridCache<Integer, String> checkCache = cache(checkIdx);

                    GridCacheEntry e = checkCache.entry(key);

                    info("Checking entry: " + e);

                    assert !e.isLocked() : "Entry is locked for grid [idx=" + checkIdx + ", entry=" + e + ']';
                }
            }
            catch (AssertionError e) {
                if (i == 2)
                    throw e;

                U.warn(log, "Check failed (will retry in 1000 ms): " + e.getMessage());

                U.sleep(1000);

                continue;
            }

            // Check passed on all grids.
            break;
        }
    }
}
