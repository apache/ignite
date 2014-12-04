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
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Basic get and transform store test.
 */
public abstract class GridCacheGetAndTransformStoreAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Cache store. */
    private static final GridCacheTestStore store = new GridCacheTestStore();

    /**
     *
     */
    protected GridCacheGetAndTransformStoreAbstractTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        store.resetTimestamp();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cache().clearAll();

        store.reset();
    }

    /** @return Caching mode. */
    protected abstract GridCacheMode cacheMode();

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(cacheMode());
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(atomicityMode());
        cc.setDistributionMode(distributionMode());
        cc.setPreloadMode(SYNC);

        cc.setStore(store);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @return Distribution mode.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected GridCacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndTransform() throws Exception {
        final AtomicBoolean finish = new AtomicBoolean();

        try {
            startGrid(0);
            startGrid(1);
            startGrid(2);

            final GridClosure<String, String> trans = new TransformClosure();

            GridFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        GridCache<Integer, String> c = cache(ThreadLocalRandom.current().nextInt(3));

                        while (!finish.get() && !Thread.currentThread().isInterrupted()) {
                            c.get(ThreadLocalRandom.current().nextInt(100));
                            c.put(ThreadLocalRandom.current().nextInt(100), "s");
                            c.transform(
                                ThreadLocalRandom.current().nextInt(100),
                                trans);
                        }

                        return null;
                    }
                },
                20);

            Thread.sleep(15_000);

            finish.set(true);

            fut.get();
        }
        finally {
            stopGrid(0);
            stopGrid(1);
            stopGrid(2);

            while (!cache().isEmpty())
                cache().globalClearAll(Long.MAX_VALUE);
        }
    }

    /**
     *
     */
    private static class TransformClosure implements GridClosure<String, String> {
        /** {@inheritDoc} */
        @Override public String apply(String s) {
            return "str";
        }
    }
}
