/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Simple cache test.
 */
public abstract class GridCacheBasicOpAbstractTest extends GridCommonAbstractTest {
    /** Grid 1. */
    private static Ignite ignite1;

    /** Grid 2. */
    private static Ignite ignite2;

    /** Grid 3. */
    private static Ignite ignite3;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3);

        ignite1 = grid(0);
        ignite2 = grid(1);
        ignite3 = grid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite1 = null;
        ignite2 = null;
        ignite3 = null;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (Ignite g : G.allGrids())
            g.cache(null).clearAll();
    }

    /**
     *
     * @throws Exception If error occur.
     */
    public void testBasicOps() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            GridCache<String, String> cache1 = ignite1.cache(null);
            GridCache<String, String> cache2 = ignite2.cache(null);
            GridCache<String, String> cache3 = ignite3.cache(null);

            ignite1.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite3.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);

            assert !cache1.containsKey("1");
            assert !cache2.containsKey("1");
            assert !cache3.containsKey("1");

            info("First put");

            cache1.put("1", "a");

            info("Start latch wait 1");

            assert latch.await(5, SECONDS);

            info("Stop latch wait 1");

            assert cache1.containsKey("1");
            assert cache2.containsKey("1");
            assert cache3.containsKey("1");

            latch = new CountDownLatch(6);

            lsnr.setLatch(latch);

            cache2.put("1", "b");
            cache3.put("1", "c");

            info("Start latch wait 2");

            assert latch.await(5, SECONDS);

            info("Stop latch wait 2");

            assert cache1.containsKey("1");
            assert cache2.containsKey("1");
            assert cache3.containsKey("1");

            latch = new CountDownLatch(3);

            lsnr.setLatch(latch);

            cache1.remove("1");

            info("Start latch wait 3");

            assert latch.await(5, SECONDS);

            info("Stop latch wait 3");

            assert !cache1.containsKey("1") : "Key set: " + cache1.keySet();
            assert !cache2.containsKey("1") : "Key set: " + cache2.keySet();
            assert !cache3.containsKey("1") : "Key set: " + cache3.keySet();
        }
        finally {
            ignite1.events().stopLocalListen(lsnr);
            ignite2.events().stopLocalListen(lsnr);
            ignite3.events().stopLocalListen(lsnr);
        }
    }

    /**
     * @throws Exception If test fails.
     */
    public void testBasicOpsAsync() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            GridCache<String, String> cache1 = ignite1.cache(null);
            GridCache<String, String> cache2 = ignite2.cache(null);
            GridCache<String, String> cache3 = ignite3.cache(null);

            ignite1.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite3.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);

            GridFuture<String> f1 = cache1.getAsync("async1");

            assert f1.get() == null;

            f1 = cache1.putAsync("async1", "asyncval1");

            assert f1.get() == null;

            f1 = cache1.getAsync("async1");

            String v1 = f1.get();

            assert v1 != null;
            assert "asyncval1".equals(v1);

            assert latch.await(5, SECONDS);

            GridFuture<String> f2 = cache2.getAsync("async1");
            GridFuture<String> f3 = cache3.getAsync("async1");

            String v2 = f2.get();
            String v3 = f3.get();

            assert v2 != null;
            assert v3 != null;

            assert "asyncval1".equals(v2);
            assert "asyncval1".equals(v3);

            lsnr.setLatch(latch = new CountDownLatch(3));

            f2 = cache2.removeAsync("async1");

            assert "asyncval1".equals(f2.get());

            assert latch.await(5, SECONDS);

            f1 = cache1.getAsync("async1");
            f2 = cache2.getAsync("async1");
            f3 = cache3.getAsync("async1");

            v1 = f1.get();
            v2 = f2.get();
            v3 = f3.get();

            info("Removed v1: " + v1);
            info("Removed v2: " + v2);
            info("Removed v3: " + v3);

            assert v1 == null;
            assert v2 == null;
            assert v3 == null;
        }
        finally {
            ignite1.events().stopLocalListen(lsnr);
            ignite2.events().stopLocalListen(lsnr);
            ignite3.events().stopLocalListen(lsnr);
        }
    }

    /**
     *
     * @throws GridException If test fails.
     */
    public void testOptimisticTransaction() throws Exception {
        CountDownLatch latch = new CountDownLatch(9);

        GridPredicate<GridEvent> lsnr = new CacheEventListener(latch);

        try {
            GridCache<String, String> cache1 = ignite1.cache(null);
            GridCache<String, String> cache2 = ignite2.cache(null);
            GridCache<String, String> cache3 = ignite3.cache(null);

            ignite1.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite2.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);
            ignite3.events().localListen(lsnr, EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED);

            GridCacheTx tx = cache1.txStart(OPTIMISTIC, READ_COMMITTED, 0, 0);

            try {
                cache1.put("tx1", "val1");
                cache1.put("tx2", "val2");
                cache1.put("tx3", "val3");

                assert cache2.get("tx1") == null;
                assert cache2.get("tx2") == null;
                assert cache2.get("tx3") == null;

                assert cache3.get("tx1") == null;
                assert cache3.get("tx2") == null;
                assert cache3.get("tx3") == null;

                tx.commit();
            }
            catch (GridException e) {
                tx.rollback();

                throw e;
            }

            assert latch.await(5, SECONDS);

            String b1 = cache2.get("tx1");
            String b2 = cache2.get("tx2");
            String b3 = cache2.get("tx3");

            String c1 = cache3.get("tx1");
            String c2 = cache3.get("tx2");
            String c3 = cache3.get("tx3");

            assert b1 != null : "Invalid value: " + b1;
            assert b2 != null : "Invalid value: " + b2;
            assert b3 != null : "Invalid value: " + b3;

            assert c1 != null : "Invalid value: " + c1;
            assert c2 != null : "Invalid value: " + c2;
            assert c3 != null : "Invalid value: " + c3;

            assert "val1".equals(b1);
            assert "val2".equals(b2);
            assert "val3".equals(b3);

            assert "val1".equals(c1);
            assert "val2".equals(c2);
            assert "val3".equals(c3);
        }
        finally {
            ignite1.events().stopLocalListen(lsnr);
            ignite2.events().stopLocalListen(lsnr);
            ignite3.events().stopLocalListen(lsnr);
        }
    }

    /**
     *
     * @throws Exception In case of error.
     */
    public void testPutWithExpiration() throws Exception {
        GridCache<String, String> cache1 = ignite1.cache(null);
        GridCache<String, String> cache2 = ignite2.cache(null);
        GridCache<String, String> cache3 = ignite3.cache(null);

        GridCacheTx tx = cache1.txStart();

        cache1.put("key", "val");

        GridCacheEntry<String, String> entry = cache1.entry("key");

        assert entry != null;

        long ttl = 500;

        entry.timeToLive(ttl);

        // Must update value for TTL to have effect.
        entry.set("val");

        assert entry.timeToLive() == ttl;

        assert cache1.get("key") != null;

        tx.commit();

        info("Going to sleep for: " + (ttl + 1000));

        // Allow for expiration.
        Thread.sleep(ttl + 1000);

        String v1 = cache1.get("key");
        String v2 = cache2.get("key");
        String v3 = cache3.get("key");

        assert v1 == null : "V1 should be null: " + v1;
        assert v2 == null : "V2 should be null: " + v2;
        assert v3 == null : "V3 should be null: " + v3;
    }

    /**
     * Event listener.
     */
    private class CacheEventListener implements GridPredicate<GridEvent> {
        /** Wait latch. */
        private CountDownLatch latch;

        /**
         * @param latch Wait latch.
         */
        CacheEventListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * @param latch New latch.
         */
        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            assert evt.type() == EVT_CACHE_OBJECT_PUT || evt.type() == EVT_CACHE_OBJECT_REMOVED :
                "Unexpected event type: " + evt;

            info("Grid cache event: " + evt);

            latch.countDown();

            return true;
        }
    }
}
