/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for {@link GridCacheMvccManager}.
 */
public class GridCacheMvccManagerSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Cache mode. */
    private GridCacheMode mode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /** @return Cache configuration. */
    protected GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(mode);
        cfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cfg.setAtomicityMode(TRANSACTIONAL);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testLocalCache() throws Exception {
        mode = LOCAL;

        testCandidates(1);
    }

    /** @throws Exception If failed. */
    public void testReplicatedCache() throws Exception {
        mode = REPLICATED;

        testCandidates(3);
    }

    /** @throws Exception If failed. */
    public void testPartitionedCache() throws Exception {
        mode = PARTITIONED;

        testCandidates(3);
    }

    /**
     * @param gridCnt Grid count.
     * @throws Exception If failed.
     */
    private void testCandidates(int gridCnt) throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(gridCnt);

            GridCache<Integer, Integer> cache = ignite.cache(null);

            GridCacheTx tx = cache.txStart();

            cache.put(1, 1);

            tx.commit();

            for (int i = 0; i < gridCnt; i++) {
                assert ((GridKernal)grid(i)).internalCache().context().mvcc().localCandidates().isEmpty();
                assert ((GridKernal)grid(i)).internalCache().context().mvcc().remoteCandidates().isEmpty();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
