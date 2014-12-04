/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Test that in {@link GridCacheMode#PARTITIONED} mode cache writes values only to the near cache store. <p/> This check
 * is needed because in current implementation if {@link GridCacheWriteBehindStore} assumes that and user store is
 * wrapped only in near cache (see {@link GridCacheProcessor} init logic).
 */
@SuppressWarnings({"unchecked"})
public class GridCachePartitionedWritesTest extends GridCommonAbstractTest {
    /** Cache store. */
    private GridCacheStore store;

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(GridCacheMode.PARTITIONED);
        cc.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setDistributionMode(NEAR_PARTITIONED);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        store = null;

        super.afterTest();
    }

    /** @throws Exception If test fails. */
    public void testWrite() throws Exception {
        final AtomicInteger putCnt = new AtomicInteger();
        final AtomicInteger rmvCnt = new AtomicInteger();

        store = new GridCacheStoreAdapter() {
            @Override public Object load(@Nullable GridCacheTx tx, Object key) {
                info(">>> Get [key=" + key + ']');

                return null;
            }

            @Override public void put(@Nullable GridCacheTx tx, Object key,
                @Nullable Object val) {
                putCnt.incrementAndGet();
            }

            @Override public void remove(@Nullable GridCacheTx tx, Object key) {
                rmvCnt.incrementAndGet();
            }
        };

        startGrid();

        GridCache<Integer, String> cache = cache();

        try {
            cache.get(1);

            GridCacheTx tx = cache.txStart();

            try {
                for (int i = 1; i <= 10; i++)
                    cache.putx(i, Integer.toString(i));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert cache.size() == 10;

            assert putCnt.get() == 10;

            tx = cache.txStart();

            try {
                for (int i = 1; i <= 10; i++) {
                    String val = cache.remove(i);

                    assert val != null;
                    assert val.equals(Integer.toString(i));
                }

                tx.commit();
            }
            finally {
                tx.close();
            }

            assert rmvCnt.get() == 10;
        }
        finally {
            stopGrid();
        }
    }
}
