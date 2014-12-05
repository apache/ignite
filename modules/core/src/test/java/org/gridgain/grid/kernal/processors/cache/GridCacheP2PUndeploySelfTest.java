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
import org.apache.ignite.marshaller.jdk.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;

/**
 *
 */
public class GridCacheP2PUndeploySelfTest extends GridCommonAbstractTest {
    /** Test p2p value. */
    private static final String TEST_VALUE = "org.gridgain.grid.tests.p2p.GridCacheDeploymentTestValue3";

    /** */
    private static final long OFFHEAP = 0;// 4 * 1024 * 1024;

    /** */
    private final TcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private final AtomicInteger idxGen = new AtomicInteger();

    /** */
    private GridCachePreloadMode mode = SYNC;

    /** */
    private boolean offheap;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setNetworkTimeout(2000);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setMarshaller(new IgniteJdkMarshaller());

        cfg.setSwapSpaceSpi(new GridFileSwapSpaceSpi());

        GridCacheConfiguration repCacheCfg = defaultCacheConfiguration();

        repCacheCfg.setName("replicated");
        repCacheCfg.setCacheMode(REPLICATED);
        repCacheCfg.setPreloadMode(mode);
        repCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        repCacheCfg.setQueryIndexEnabled(false);
        repCacheCfg.setAtomicityMode(TRANSACTIONAL);

        if (offheap)
            repCacheCfg.setOffHeapMaxMemory(OFFHEAP);
        else
            repCacheCfg.setSwapEnabled(true);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");
        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setPreloadMode(mode);
        partCacheCfg.setAffinity(new GridCacheModuloAffinityFunction(11, 1));
        partCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        partCacheCfg.setEvictNearSynchronized(false);
        partCacheCfg.setQueryIndexEnabled(false);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        if (offheap)
            partCacheCfg.setOffHeapMaxMemory(OFFHEAP);
        else
            partCacheCfg.setSwapEnabled(true);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg);

        cfg.setDeploymentMode(SHARED);
        cfg.setPeerClassLoadingLocalClassPathExclude(GridCacheP2PUndeploySelfTest.class.getName());

        cfg.setUserAttributes(F.asMap(GridCacheModuloAffinityFunction.IDX_ATTR, idxGen.getAndIncrement()));

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testSwapP2PReplicated() throws Exception {
        offheap = false;

        checkP2PUndeploy("replicated");
    }

    /** @throws Exception If failed. */
    public void testOffHeapP2PReplicated() throws Exception {
        offheap = true;

        checkP2PUndeploy("replicated");
    }

    /** @throws Exception If failed. */
    public void testSwapP2PPartitioned() throws Exception {
        offheap = false;

        checkP2PUndeploy("partitioned");
    }

    /** @throws Exception If failed. */
    public void testOffheapP2PPartitioned() throws Exception {
        offheap = true;

        checkP2PUndeploy("partitioned");
    }

    /** @throws Exception If failed. */
    public void testSwapP2PReplicatedNoPreloading() throws Exception {
        mode = NONE;
        offheap = false;

        checkP2PUndeploy("replicated");
    }

    /** @throws Exception If failed. */
    public void testOffHeapP2PReplicatedNoPreloading() throws Exception {
        mode = NONE;
        offheap = true;

        checkP2PUndeploy("replicated");
    }

    /** @throws Exception If failed. */
    public void testSwapP2PPartitionedNoPreloading() throws Exception {
        mode = NONE;
        offheap = false;

        checkP2PUndeploy("partitioned");
    }

    /** @throws Exception If failed. */
    public void testOffHeapP2PPartitionedNoPreloading() throws Exception {
        mode = NONE;
        offheap = true;

        checkP2PUndeploy("partitioned");
    }

    /**
     * @param cacheName Cache name.
     * @param g Grid.
     * @return Size.
     * @throws GridException If failed.
     */
    private long size(String cacheName, GridKernal g) throws GridException {
        if (offheap)
            return g.cache(cacheName).offHeapEntriesCount();

        return g.context().swap().swapSize(swapSpaceName(cacheName, g));
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkP2PUndeploy(String cacheName) throws Exception {
        assert !F.isEmpty(cacheName);

        ClassLoader ldr = getExternalClassLoader();

        Class valCls = ldr.loadClass(TEST_VALUE);

        try {
            Ignite ignite1 = startGrid(1);
            GridKernal grid2 = (GridKernal)startGrid(2);

            GridCache<Integer, Object> cache1 = ignite1.cache(cacheName);
            GridCache<Integer, Object> cache2 = grid2.cache(cacheName);

            Object v1 = valCls.newInstance();

            cache1.put(1, v1);
            cache1.put(2, valCls.newInstance());
            cache1.put(3, valCls.newInstance());
            cache1.put(4, valCls.newInstance());

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            assert v2 != null;

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert v2.toString().equals(v1.toString());
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader");

            assert cache2.evict(2);
            assert cache2.evict(3);
            assert cache2.evict(4);

            long swapSize = size(cacheName, grid2);

            info("Swap size: " + swapSize);

            assert swapSize > 0;

            stopGrid(1);

            assert waitCacheEmpty(cache2, 10000);

            for (int i = 0; i < 3; i++) {
                swapSize = size(cacheName, grid2);

                if (swapSize > 0) {
                    if (i < 2) {
                        U.warn(log, "Swap size check failed (will retry in 1000 ms): " + swapSize);

                        U.sleep(1000);

                        continue;
                    }

                    fail("Swap size check failed: " + swapSize);
                }
                else if (swapSize == 0)
                    break;
                else
                    assert false : "Negative swap size: " + swapSize;
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param cacheName Cache name.
     * @param grid Kernal.
     * @return Name for swap space.
     */
    private String swapSpaceName(String cacheName, GridKernal grid) {
        GridCacheContext<Object, Object> cctx = grid.internalCache(cacheName).context();

        return CU.swapSpaceName(cctx.isNear() ? cctx.near().dht().context() : cctx);
    }

    /**
     * @param cache Cache.
     * @param timeout Timeout.
     * @return {@code True} if success.
     * @throws InterruptedException If thread was interrupted.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean waitCacheEmpty(GridCacheProjection<Integer, Object> cache, long timeout)
        throws InterruptedException {
        assert cache != null;
        assert timeout >= 0;

        long end = System.currentTimeMillis() + timeout;

        while (end - System.currentTimeMillis() >= 0) {
            if (cache.isEmpty())
                return true;

            Thread.sleep(500);
        }

        return cache.isEmpty();
    }
}
