/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache atomic long api test.
 */
public class GridCacheAtomicLongApiSelfTest extends GridCommonAbstractTest {
    /** Random number generator. */
    private static final Random RND = new Random();

    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration repCacheCfg = defaultCacheConfiguration();

        repCacheCfg.setName("replicated");
        repCacheCfg.setCacheMode(REPLICATED);
        repCacheCfg.setPreloadMode(SYNC);
        repCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        repCacheCfg.setEvictionPolicy(null);
        repCacheCfg.setAtomicityMode(TRANSACTIONAL);
        repCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");
        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        GridCacheConfiguration locCacheCfg = defaultCacheConfiguration();

        locCacheCfg.setName("local");
        locCacheCfg.setCacheMode(LOCAL);
        locCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        locCacheCfg.setEvictionPolicy(null);
        locCacheCfg.setAtomicityMode(TRANSACTIONAL);
        locCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg, locCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateRemove() throws Exception {
        createRemove("local");

        createRemove("replicated");

        createRemove("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void createRemove(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        String atomicName1 = "FIRST";

        String atomicName2 = "SECOND";

        GridCacheAtomicLong atomic1 = cache.dataStructures().atomicLong(atomicName1, 0, true);
        GridCacheAtomicLong atomic2 = cache.dataStructures().atomicLong(atomicName2, 0, true);
        GridCacheAtomicLong atomic3 = cache.dataStructures().atomicLong(atomicName1, 0, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);
        assertNotNull(atomic3);

        assert atomic1.equals(atomic3);
        assert atomic3.equals(atomic1);
        assert !atomic3.equals(atomic2);

        assertEquals(0, cache.primarySize());

        assert cache.dataStructures().removeAtomicLong(atomicName1);
        assert cache.dataStructures().removeAtomicLong(atomicName2);
        assert !cache.dataStructures().removeAtomicLong(atomicName1);
        assert !cache.dataStructures().removeAtomicLong(atomicName2);

        assertEquals(0, cache.primarySize());

        try {
            atomic1.get();

            fail();
        }
        catch (GridException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementAndGet() throws Exception {
        incrementAndGet("local");

        incrementAndGet("replicated");

        incrementAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void incrementAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal + 1 == atomic.incrementAndGet();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndIncrement() throws Exception {
        getAndIncrement("local");

        getAndIncrement("replicated");

        getAndIncrement("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndIncrement(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndIncrement();
        assert curAtomicVal + 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecrementAndGet() throws Exception {
        decrementAndGet("local");

        decrementAndGet("replicated");

        decrementAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void decrementAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal - 1 == atomic.decrementAndGet();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndDecrement() throws Exception {
        getAndDecrement("local");

        getAndDecrement("replicated");

        getAndDecrement("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndDecrement(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndDecrement();
        assert curAtomicVal - 1 == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndAdd() throws Exception {
        getAndAdd("local");

        getAndAdd("replicated");

        getAndAdd("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndAdd(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndAdd(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddAndGet() throws Exception {
        addAndGet("local");

        addAndGet("replicated");

        addAndGet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void addAndGet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long delta = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal + delta == atomic.addAndGet(delta);
        assert curAtomicVal + delta == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndSet() throws Exception {
        getAndSet("local");

        getAndSet("replicated");

        getAndSet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndSet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long newVal = RND.nextLong();

        long curAtomicVal = atomic.get();

        assert curAtomicVal == atomic.getAndSet(newVal);
        assert newVal == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCompareAndSet() throws Exception {
        compareAndSet("local");

        compareAndSet("replicated");

        compareAndSet("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"NullableProblems", "ConstantConditions"})
    private void compareAndSet(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        long newVal = RND.nextLong();

        final long oldVal = atomic.get();

        // Don't set new value.
        assert !atomic.compareAndSet(oldVal - 1, newVal);

        assert oldVal == atomic.get();

        // Set new value.
        assert atomic.compareAndSet(oldVal, newVal);

        assert newVal == atomic.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndSetInTx() throws Exception {
        getAndSetInTx("local");

        getAndSetInTx("replicated");

        getAndSetInTx("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getAndSetInTx(String cacheName) throws Exception {
        info("Running test [name=" + getName() + ", cache=" + cacheName + ']');

        GridCache cache = grid().cache(cacheName);

        assertNotNull(cache);
        assertEquals(0, cache.primarySize());

        GridCacheAtomicLong atomic = cache.dataStructures().atomicLong("atomic", 0, true);

        assertEquals(0, cache.primarySize());

        try (GridCacheTx tx = cache.txStart()) {
            long newVal = RND.nextLong();

            long curAtomicVal = atomic.get();

            assert curAtomicVal == atomic.getAndSet(newVal);
            assert newVal == atomic.get();
        }
    }
}
