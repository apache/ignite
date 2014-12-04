/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test cases for multi-threaded tests.
 */
public abstract class GridCacheMultiNodeLockAbstractTest extends GridCommonAbstractTest {
    /** Grid 1. */
    private static Ignite ignite1;

    /** Grid 2. */
    private static Ignite ignite2;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Listeners. */
    private static Collection<GridPredicate<GridEvent>> lsnrs = new ArrayList<>();

    /**
     *
     */
    protected GridCacheMultiNodeLockAbstractTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        return cfg;
    }

    /**
     * @return {@code True} for partitioned caches.
     */
    protected boolean partitioned() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);

        startGrid(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        ignite1 = null;
        ignite2 = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        removeListeners(ignite1);
        removeListeners(ignite2);

        lsnrs.clear();

        for (int i = 1; i <= 3; i++) {
            cache(i).clearAll();

            assertTrue(
                "Cache isn't empty [i=" + i + ", entries=" + ((GridKernal)grid(i)).internalCache().entries() + "]",
                cache(i).isEmpty());
        }
    }

    /**
     * @param ignite Grid to remove listeners from.
     */
    private void removeListeners(Ignite ignite) {
        for (GridPredicate<GridEvent> lsnr : lsnrs)
            ignite.events().stopLocalListen(lsnr);
    }

    /**
     * @param ignite Grid
     * @param lsnr Listener.
     */
    void addListener(Ignite ignite, GridPredicate<GridEvent> lsnr) {
        if (!lsnrs.contains(lsnr))
            lsnrs.add(lsnr);

        ignite.events().localListen(lsnr, EVTS_CACHE);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkLocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void checkRemoteLocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert cache.isLocked(key);
        assert !cache.isLockedByThread(key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    @SuppressWarnings({"BusyWait"})
    private void checkUnlocked(GridCacheProjection<Integer,String> cache, Integer key) {
        assert !cache.isLockedByThread(key);

        if (partitioned()) {
            for(int i = 0; i < 200; i++)
                if (cache.isLocked(key)) {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else
                    return;
        }

        assertFalse("Key locked [key=" + key + ", entries=" + entries(key) + "]", cache.isLocked(key));
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkLocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys) {
            checkLocked(cache, key);
        }
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkRemoteLocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys) {
            checkRemoteLocked(cache, key);
        }
    }

    /**
     *
     * @param cache Cache.
     * @param keys Keys.
     */
    private void checkUnlocked(GridCacheProjection<Integer,String> cache, Iterable<Integer> keys) {
        for (Integer key : keys)
            checkUnlocked(cache, key);
    }

    /**
     *
     * @throws Exception If test failed.
     */
    public void testBasicLock() throws Exception {
        GridCache<Integer, String> cache = ignite1.cache(null);

        assert cache.lock(1, 0L);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        cache.unlockAll(F.asList(1));

        checkUnlocked(cache, 1);
    }

    /**
     * Entries for key.
     *
     * @param key Key.
     * @return Entries.
     */
    private String entries(int key) {
        if (partitioned()) {
            GridNearCacheAdapter<Integer, String> near1 = near(1);
            GridNearCacheAdapter<Integer, String> near2 = near(2);

            GridDhtCacheAdapter<Integer, String> dht1 = dht(1);
            GridDhtCacheAdapter<Integer, String> dht2 = dht(2);

            return "Entries [ne1=" + near1.peekEx(key) + ", de1=" + dht1.peekEx(key) + ", ne2=" + near2.peekEx(key) +
                ", de2=" + dht2.peekEx(key) + ']';
        }

        return "Entries [e1=" + ignite1.cache(null).entry(key) + ", e2=" + ignite2.cache(null).entry(key) + ']';
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLock() throws Exception {
        GridCache<Integer, String> cache1 = ignite1.cache(null);
        GridCache<Integer, String> cache2 = ignite2.cache(null);

        assert cache1.lock(1, 0L);

        assert cache1.isLocked(1) : entries(1);
        assert cache1.isLockedByThread(1);

        assert cache2.isLocked(1) : entries(1);
        assert !cache2.isLockedByThread(1);

        try {
            assert !cache2.lock(1, -1L);

            assert cache2.isLocked(1) : entries(1);
            assert !cache2.isLockedByThread(1);
        }
        finally {
            cache1.unlock(1);

            checkUnlocked(cache1, 1);
        }

        assert cache2.lock(1, 0L);

        assert cache2.isLocked(1) : entries(1);
        assert cache2.isLockedByThread(1);

        assert cache1.isLocked(1) : entries(1);
        assert !cache1.isLockedByThread(1);

        CountDownLatch latch = new CountDownLatch(1);

        addListener(ignite1, new UnlockListener(latch, 1));

        try {
            assert !cache1.lock(1, -1L);

            assert cache1.isLocked(1) : entries(1);
            assert !cache1.isLockedByThread(1);
        }
        finally {
            cache2.unlockAll(F.asList(1));
        }

        latch.await();

        checkUnlocked(cache1, 1);
        checkUnlocked(cache2, 1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockAsync() throws Exception {
        GridCache<Integer, String> cache1 = ignite1.cache(null);
        GridCache<Integer, String> cache2 = ignite2.cache(null);

        assert cache1.lockAsync(1, 0L).get();

        try {
            assert cache1.isLocked(1);
            assert cache1.isLockedByThread(1);

            assert cache2.isLocked(1);
            assert !cache2.isLockedByThread(1);

            assert !cache2.lockAsync(1, -1L).get();
        }
        finally {
            cache1.unlockAll(F.asList(1));
        }

        checkUnlocked(cache1, 1);

        assert cache2.lockAsync(1, 0L).get();

        CountDownLatch latch = new CountDownLatch(1);

        addListener(ignite1, new UnlockListener(latch, 1));

        try {
            assert cache1.isLocked(1);
            assert !cache1.isLockedByThread(1);

            assert cache2.isLocked(1);
            assert cache2.isLockedByThread(1);

            assert !cache1.lockAsync(1, -1L).get();
        }
        finally {
            cache2.unlockAll(F.asList(1));
        }

        latch.await();

        assert !cache1.isLocked(1);
        assert !cache1.isLockedByThread(1);

        checkUnlocked(cache1, 1);
        checkUnlocked(cache2, 1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockWithKeyLists() throws Exception {
        GridCache<Integer, String> cache1 = ignite1.cache(null);
        GridCache<Integer, String> cache2 = ignite2.cache(null);

        Collection<Integer> keys1 = new ArrayList<>();
        Collection<Integer> keys2 = new ArrayList<>();

        Collections.addAll(keys1, 1, 2, 3);
        Collections.addAll(keys2, 2, 3, 4);

        assert cache1.lockAll(keys1, 0);

        checkLocked(cache1, keys1);

        try {
            assert !cache2.lockAll(keys2, -1);

            assert cache2.isLocked(2);
            assert cache2.isLocked(3);

            checkUnlocked(cache1, 4);
            checkUnlocked(cache2, 4);

            assert !cache2.isLockedByThread(2);
            assert !cache2.isLockedByThread(3);
            assert !cache2.isLockedByThread(4);
        }
        finally {
            cache1.unlockAll(keys1);
        }

        checkUnlocked(cache1, keys1);

        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, 4);

        assert cache2.lockAll(keys2, 0);

        CountDownLatch latch1 = new CountDownLatch(keys2.size());
        CountDownLatch latch2 = new CountDownLatch(1);

        addListener(ignite2, new UnlockListener(latch2, 1));
        addListener(ignite1, (new UnlockListener(latch1, keys2)));

        try {
            checkLocked(cache2, keys2);

            checkUnlocked(cache2, 1);

            assert cache1.lock(1, -1L);

            checkLocked(cache1, 1);

            checkRemoteLocked(cache1, keys2);

            checkRemoteLocked(cache2, 1);
        }
        finally {
            cache2.unlockAll(keys2);

            cache1.unlockAll(F.asList(1));
        }

        latch1.await();
        latch2.await();

        checkUnlocked(cache1, keys1);
        checkUnlocked(cache2, keys1);
        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, keys2);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiNodeLockAsyncWithKeyLists() throws Exception {
        GridCache<Integer, String> cache1 = ignite1.cache(null);
        GridCache<Integer, String> cache2 = ignite2.cache(null);

        Collection<Integer> keys1 = new ArrayList<>();
        Collection<Integer> keys2 = new ArrayList<>();

        Collections.addAll(keys1, 1, 2, 3);
        Collections.addAll(keys2, 2, 3, 4);

        GridFuture<Boolean> f1 = cache1.lockAllAsync(keys1, 0);

        try {
            assert f1.get();

            checkLocked(cache1, keys1);

            checkRemoteLocked(cache2, keys1);

            GridFuture<Boolean> f2 = cache2.lockAllAsync(keys2, -1);

            assert !f2.get();

            checkLocked(cache1, keys1);

            checkUnlocked(cache1, 4);
            checkUnlocked(cache2, 4);

            checkRemoteLocked(cache2, keys1);
        }
        finally {
            cache1.unlockAll(keys1);
        }

        checkUnlocked(cache1, keys1);

        CountDownLatch latch = new CountDownLatch(keys2.size());

        addListener(ignite1, new UnlockListener(latch, keys2));

        GridFuture<Boolean> f2 = cache2.lockAllAsync(keys2, 0);

        try {
            assert f2.get();

            checkLocked(cache2, keys2);

            checkRemoteLocked(cache1, keys2);

            checkUnlocked(cache1, 1);

            f1 = cache1.lockAllAsync(keys2, -1);

            assert !f1.get();

            checkLocked(cache2, keys2);

            checkUnlocked(cache1, 1);
            checkUnlocked(cache2 , 1);

            checkRemoteLocked(cache1, keys2);
        }
        finally {
            cache2.unlockAll(keys2);
        }

        latch.await();

        checkUnlocked(cache1, keys1);
        checkUnlocked(cache2, keys1);
        checkUnlocked(cache1, keys2);
        checkUnlocked(cache2, keys2);
    }

    /**
     * @throws GridException If test failed.
     */
    public void testLockReentry() throws GridException {
        GridCache<Integer, String> cache = ignite1.cache(null);

        assert cache.lock(1, 0L);

        try {
            checkLocked(cache, 1);

            assert cache.lock(1, 0L);

            checkLocked(cache, 1);

            cache.unlockAll(F.asList(1));

            checkLocked(cache, 1);
        }
        finally {
            cache.unlockAll(F.asList(1));
        }

        checkUnlocked(cache, 1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockMultithreaded() throws Exception {
        final GridCache<Integer, String> cache = ignite1.cache(null);

        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                assert cache.lock(1, 0L);

                info("After lock for key 1");

                try {
                    checkLocked(cache, 1);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    // Reentry.
                    assert cache.lock(1, 0L);

                    checkLocked(cache, 1);

                    // Nested lock.
                    assert cache.lock(2, -1L);

                    checkLocked(cache, 2);

                    // Unlock reentry.
                    cache.unlockAll(F.asList(1));

                    // Outer should still be owned.
                    checkLocked(cache, 1);

                    // Unlock in reverse order.
                    cache.unlockAll(F.asList(2));

                    checkUnlocked(cache, 2);

                    l2.await();

                    info("Waited for latch 2");
                }
                finally {
                    cache.unlockAll(F.asList(1));

                    info("Unlocked entry for key 1.");
                }

                assert !cache.isLockedByThread(1);
                assert !cache.isLockedByThread(2);

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

                info("Tried to lock cache for key1");

                l2.countDown();

                info("Released latch2");

                assert cache.lock(1, 0L);

                try {
                    info("Locked cache for key 1");

                    checkLocked(cache, 1);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    cache.unlockAll(F.asList(1));

                    info("Unlocked cache for key 1");
                }

                checkUnlocked(cache, 1);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        checkUnlocked(cache, 1);
        checkUnlocked(cache, 2);
    }

    /**
     * Cache unlock listener.
     */
    private class UnlockListener implements GridPredicate<GridEvent> {
        /** Latch. */
        private final CountDownLatch latch;

        /** */
        private final Collection<Integer> keys;

        /**
         * @param latch Latch.
         * @param keys Keys.
         */
        UnlockListener(CountDownLatch latch, Integer... keys) {
            this(latch, Arrays.asList(keys));
        }

        /**
         * @param latch Latch.
         * @param keys Keys.
         */
        UnlockListener(CountDownLatch latch, Collection<Integer> keys) {
            assert latch != null;
            assert keys != null;

            this.latch = latch;
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(GridEvent evt) {
            info("Received cache event: " + evt);

            if (evt instanceof GridCacheEvent) {
                GridCacheEvent cacheEvt = (GridCacheEvent)evt;

                Integer key = cacheEvt.key();

                if (keys.contains(key))
                    if (evt.type() == EVT_CACHE_OBJECT_UNLOCKED)
                        latch.countDown();
            }

            return true;
        }
    }
}
