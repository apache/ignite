/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Checks that exception is propagated to user when cache store throws an exception.
 */
public class GridCacheGetStoreErrorSelfTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Near enabled flag. */
    private boolean nearEnabled;

    /** Cache mode for test. */
    private GridCacheMode cacheMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode);
        cc.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cc.setAtomicityMode(TRANSACTIONAL);

        cc.setStore(new GridCacheStoreAdapter<Object, Object>() {
            @Override public Object load(@Nullable GridCacheTx tx, Object key)
                throws GridException {
                throw new GridException("Failed to get key from store: " + key);
            }

            @Override public void put(@Nullable GridCacheTx tx, Object key,
                @Nullable Object val) {
                // No-op.
            }

            @Override public void remove(@Nullable GridCacheTx tx, Object key) {
                // No-op.
            }
        });

        c.setCacheConfiguration(cc);

        c.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        return c;
    }

    /** @throws Exception If failed. */
    public void testGetErrorNear() throws Exception {
        checkGetError(true, PARTITIONED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorColocated() throws Exception {
        checkGetError(false, PARTITIONED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorReplicated() throws Exception {
        checkGetError(false, REPLICATED);
    }

    /** @throws Exception If failed. */
    public void testGetErrorLocal() throws Exception {
        checkGetError(false, LOCAL);
    }

    /** @throws Exception If failed. */
    private void checkGetError(boolean nearEnabled, GridCacheMode cacheMode) throws Exception {
        this.nearEnabled = nearEnabled;
        this.cacheMode = cacheMode;

        startGrids(3);

        try {
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid(0).cache(null).get(nearKey());

                    return null;
                }
            }, GridException.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /** @return Key that is not primary nor backup for grid 0. */
    private String nearKey() {
        String key = "";

        for (int i = 0; i < 1000; i++) {
            key = String.valueOf(i);

            GridCacheEntry<Object, Object> entry = grid(0).cache(null).entry(key);

            if (!entry.primary() && entry.backup())
                break;
        }

        return key;
    }
}
