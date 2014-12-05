/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 *
 */
public abstract class GridCacheRefreshAheadAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final TestStore store = new TestStore();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /**
     * @throws GridException If test failed.
     */
    public void testReadAhead() throws Exception {
        store.testThread(Thread.currentThread());

        GridCache<Integer, String> cache = grid(0).cache(null);

        GridCacheEntry<Integer, String> entry = cache.entry(1);

        assert entry != null;

        entry.timeToLive(1000);

        entry.set("1");

        Thread.sleep(600);

        store.startAsyncLoadTracking();

        cache.get(1);

        assert store.wasAsynchronousLoad() : "No async loads were performed on the store: " + store;
    }

    /**
     * @return Test store.
     */
    protected TestStore testStore() {
        return store;
    }

    /**
     * @return Grid count for test.
     */
    protected abstract int gridCount();

    /**
     * Test cache store.
     */
    private class TestStore extends GridCacheStoreAdapter<Object, Object> {
        /** */
        private volatile Thread testThread;

        /** */
        private volatile boolean wasAsyncLoad;

        /** */
        @GridToStringExclude
        private final CountDownLatch latch = new CountDownLatch(1);

        /** */
        private volatile boolean trackLoads;

        /**
         * @return true if was asynchronous load.
         * @throws Exception If an error occurs.
         */
        public boolean wasAsynchronousLoad() throws Exception {
            return latch.await(3, SECONDS) && wasAsyncLoad;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object load(GridCacheTx tx, Object key) throws GridException {
            if (trackLoads) {
                wasAsyncLoad = wasAsyncLoad || !testThread.equals(Thread.currentThread());

                if (wasAsyncLoad)
                    latch.countDown();

                info("Load call was tracked on store: " + this);
            }
            else
                info("Load call was not tracked on store: " + this);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void put(GridCacheTx tx, Object key, Object val) throws GridException {
            /* No-op. */
        }

        /** {@inheritDoc} */
        @Override public void remove(GridCacheTx tx, Object key) throws GridException {
            /* No-op. */
        }

        /**
         * @param testThread Thread that runs test.
         */
        public void testThread(Thread testThread) {
            this.testThread = testThread;
        }

        /**
         *
         */
        public void startAsyncLoadTracking() {
            trackLoads = true;
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(TestStore.class, this);
        }
    }
}
