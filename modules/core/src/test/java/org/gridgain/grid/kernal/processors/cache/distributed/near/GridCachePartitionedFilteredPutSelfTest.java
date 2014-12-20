/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;

/**
 * Test filtered put.
 */
public class GridCachePartitionedFilteredPutSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Discovery SPI;
     */
    private DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setDistributionMode(NEAR_PARTITIONED);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredPutCheckNear() throws Exception {
        doFilteredPut();

        GridCache<Integer, Integer> c = cache();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFilteredPutCheckDht() throws Exception {
        doFilteredPut();

        GridCache<Integer, Integer> c =
            ((GridNearCacheAdapter<Integer, Integer>)cache().<Integer, Integer>cache()).dht();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAndRollbackCheckNear() throws Exception {
        doPutAndRollback();

        GridCache<Integer, Integer> c = cache();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAndRollbackCheckDht() throws Exception {
        doPutAndRollback();

        GridCache<Integer, Integer> c =
            ((GridNearCacheAdapter<Integer, Integer>)cache().<Integer, Integer>cache()).dht();

        assert c.entrySet().isEmpty() : "Actual size: " + c.entrySet().size();
    }

    /**
     * @throws Exception If failed.
     */
    private void doFilteredPut() throws Exception {
        GridCache<Integer, Integer> c = cache();

        try (GridCacheTx tx = c.txStart()) {
            assert !c.putx(1, 1, F.<Integer, Integer>cacheHasPeekValue());

            tx.commit();
        }

        assert c.isEmpty();
        assert c.peek(1) == null;
        assert c.get(1) == null;
    }

    /**
     * @throws Exception If failed.
     */
    private void doPutAndRollback() throws Exception {
        GridCache<Integer, Integer> c = cache();

        try (GridCacheTx tx = c.txStart()) {
            assert c.putx(1, 1);

            tx.rollback();
        }

        assert c.isEmpty();
        assert c.peek(1) == null;
        assert c.get(1) == null;
    }
}
