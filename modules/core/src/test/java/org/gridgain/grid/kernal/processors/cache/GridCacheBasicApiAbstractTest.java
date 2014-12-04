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
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Test cases for multi-threaded tests.
 */
public abstract class GridCacheBasicApiAbstractTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     *
     */
    protected GridCacheBasicApiAbstractTest() {
        super(true /*start grid.*/);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testBasicLock() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        assert cache.lock(1, 0);

        assert cache.isLocked(1);

        cache.unlock(1);

        assert !cache.isLocked(1);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testSingleLockReentry() throws GridException {
        GridCache<Integer, String> cache = ignite.cache(null);

        assert cache.lock(1, 0);

        try {
            assert cache.isLockedByThread(1);

            assert cache.lock(1, 0);

            cache.unlock(1);

            assert cache.isLockedByThread(1);
        }
        finally {
            cache.unlock(1);
        }

        assert !cache.isLockedByThread(1);
        assert !cache.isLocked(1);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testReentry() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        assert cache.lock(1, 0);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        assert cache.lock(1, 0);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        assert cache.lock(1, 0);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        cache.unlock(1);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        cache.unlock(1);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        cache.unlock(1);

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testManyLockReentries() throws GridException {
        GridCache<Integer, String> cache = ignite.cache(null);

        Integer key = 1;

        assert cache.lock(key, 0);

        try {
            assert cache.get(key) == null;
            assert cache.put(key, "1") == null;
            assert "1".equals(cache.get(key));

            assert cache.isLocked(key);
            assert cache.isLockedByThread(key);

            assert cache.lock(key, 0);

            assert cache.isLocked(key);
            assert cache.isLockedByThread(key);

            try {
                assert "1".equals(cache.remove(key));
            }
            finally {
                cache.unlock(key);
            }

            assert cache.isLocked(key);
            assert cache.isLockedByThread(key);
        }
        finally {
            cache.unlock(key);

            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockMultithreaded() throws Exception {
        final GridCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                assert cache.lock(1, 0);

                info("After lock for key 1");

                try {
                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    // Reentry.
                    assert cache.lock(1, -1L);

                    // Nested lock.
                    assert cache.lock(2, -1L);

                    l2.await();

                    cache.unlock(1);

                    // Unlock in reverse order.
                    cache.unlock(2);

                    info("Waited for latch 2");
                }
                finally {
                    cache.unlock(1);

                    info("Unlocked entry for key 1.");
                }

                l3.countDown();

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                info("Latch1 released.");

                assert !cache.lock(1, -1L);

                if (!cache.isLocked(1))
                    throw new IllegalArgumentException();

                assert !cache.isLockedByThread(1);

                info("Tried to lock cache for key1");

                l2.countDown();

                info("Released latch2");

                l3.await();

                assert cache.lock(1, -1L);

                try {
                    info("Locked cache for key 1");

                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    cache.unlock(1);

                    info("Unlocked cache for key 1");
                }

                assert !cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLocked(1);
    }

    /**
     *
     * @throws Exception If error occur.
     */
    public void testBasicOps() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        CountDownLatch latch = new CountDownLatch(1);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            ignite.events().localListen(lsnr, EVTS_CACHE);

            int key = (int)System.currentTimeMillis();

            assert !cache.containsKey(key);

            cache.put(key, "a");

            info("Start latch wait 1");

            latch.await();

            info("Stop latch wait 1");

            assert cache.containsKey(key);

            latch = new CountDownLatch(2);

            lsnr.latch(latch);

            cache.put(key, "b");
            cache.put(key, "c");

            info("Start latch wait 2");

            latch.await();

            info("Stop latch wait 2");

            assert cache.containsKey(key);

            latch = new CountDownLatch(1);

            lsnr.latch(latch);

            cache.remove(key);

            info("Start latch wait 3");

            latch.await();

            info("Stop latch wait 3");

            assert !cache.containsKey(key);
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     * @throws Exception If error occur.
     */
    public void testBasicOpsWithReentry() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        int key = (int)System.currentTimeMillis();

        assert !cache.containsKey(key);

        assert cache.lock(key, 0);

        CountDownLatch latch = new CountDownLatch(1);

        CacheEventListener lsnr = new CacheEventListener(latch);

        try {
            ignite.events().localListen(lsnr, EVTS_CACHE);

            cache.put(key, "a");

            info("Start latch wait 1");

            latch.await();

            info("Stop latch wait 1");

            assert cache.containsKey(key);
            assert cache.isLockedByThread(key);

            latch = new CountDownLatch(2);

            lsnr.latch(latch);

            cache.put(key, "b");
            cache.put(key, "c");

            info("Start latch wait 2");

            latch.await();

            info("Stop latch wait 2");

            assert cache.containsKey(key);
            assert cache.isLockedByThread(key);

            latch = new CountDownLatch(1);

            lsnr.latch(latch);

            cache.remove(key);

            info("Start latch wait 3");

            latch.await();

            info("Stop latch wait 3");

            assert cache.keySet().contains(key);
            assert cache.isLocked(key);
        }
        finally {
            cache.unlock(key);

            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }

        // Entry should be evicted since allowEmptyEntries is false.
        assert !cache.keySet().contains(key) : "Key set: " + cache.keySet();
        assert !cache.isLocked(key);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultiLocks() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        Collection<Integer> keys = new ArrayList<>(3);

        Collections.addAll(keys, 1, 2, 3);

        assert cache.lockAll(keys, 0);

        assert cache.isLocked(1);
        assert cache.isLocked(2);
        assert cache.isLocked(3);

        assert cache.isLockedByThread(1);
        assert cache.isLockedByThread(2);
        assert cache.isLockedByThread(3);

        cache.unlockAll(keys);

        assert !cache.isLocked(1);
        assert !cache.isLocked(2);
        assert !cache.isLocked(3);

        assert !cache.isLockedByThread(1);
        assert !cache.isLockedByThread(2);
        assert !cache.isLockedByThread(3);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testGetPutRemove() throws GridException {
        GridCache<Integer, String> cache = ignite.cache(null);

        int key = (int)System.currentTimeMillis();

        assert cache.get(key) == null;
        assert cache.put(key, "1") == null;

        String val = cache.get(key);

        assert val != null;
        assert "1".equals(val);

        val = cache.remove(key);

        assert val != null;
        assert "1".equals(val);
        assert cache.get(key) == null;
    }

    /**
     *
     * @throws Exception In case of error.
     */
    public void testPutWithExpiration() throws Exception {
        GridCache<Integer, String> cache = ignite.cache(null);

        CacheEventListener lsnr = new CacheEventListener(new CountDownLatch(1));

        ignite.events().localListen(lsnr, EVTS_CACHE);

        try {
            int key = (int)System.currentTimeMillis();

            GridCacheEntry<Integer, String> entry = cache.entry(key);

            entry.timeToLive(200);

            entry.set("val");

            assert cache.get(key) != null;

            entry.timeToLive(200);

            // Must update for TTL to have effect.
            entry.set("val");

            Thread.sleep(500);

            assert cache.get(key) == null;
        }
        finally {
            ignite.events().stopLocalListen(lsnr, EVTS_CACHE);
        }
    }

    /**
     * Event listener.
     */
    private class CacheEventListener implements IgnitePredicate<GridEvent> {
        /** Wait latch. */
        private CountDownLatch latch;

        /** Event types. */
        private int[] types;

        /**
         * @param latch Wait latch.
         * @param types Event types.
         */
        CacheEventListener(CountDownLatch latch, int... types) {
            this.latch = latch;
            this.types = types;

            if (F.isEmpty(types))
                this.types = new int[] { EVT_CACHE_OBJECT_PUT, EVT_CACHE_OBJECT_REMOVED };
        }

        /**
         * @param latch New latch.
         */
        void latch(CountDownLatch latch) {
            this.latch = latch;
        }

        /**
         * Waits for latch.
         *
         * @throws InterruptedException If got interrupted.
         */
        void await() throws InterruptedException {
            latch.await();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            info("Grid cache event: " + evt);

            if (U.containsIntArray(types, evt.type()))
                latch.countDown();

            return true;
        }
    }
}
