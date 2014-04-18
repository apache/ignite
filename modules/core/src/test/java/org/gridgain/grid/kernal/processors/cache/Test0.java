package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.ATOMIC;
import static org.gridgain.grid.cache.GridCacheMode.PARTITIONED;

/**
 * Multi-threaded tests for cache queries.
 */
@SuppressWarnings("StatementWithEmptyBody")
public class Test0 extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final long DURATION = 30 * 1000 * 10;

    @Override protected long getTestTimeout() {
        return DURATION * 10;
    }

    /** Don't start grid by default. */
    public Test0() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setSwapSpaceSpi(new GridFileSwapSpaceSpi());
        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setAtomicityMode(ATOMIC);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setSwapEnabled(true);
        cacheCfg.setBackups(1);
        cacheCfg.setEvictionPolicy(new GridCacheLruEvictionPolicy(100));
        cacheCfg.setOffHeapMaxMemory(10000);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testMultiThreadedSwapUnswapObject() throws Exception {
        final int keyCnt = 10000;
        final int valCnt = 10000;

        final Grid g = grid(0);

        final GridCache<Integer, TestValue> c = g.cache(null);

        Random rnd = new Random();

        for (;;)
            c.putx(rnd.nextInt(keyCnt), new TestValue(rnd.nextInt(valCnt)));
    }

    /**
     * Test value.
     */
    private static class TestValue implements Serializable {
        /** Value. */
        @GridCacheQuerySqlField
        private int val;

        /**
         * @param val Value.
         */
        private TestValue(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
