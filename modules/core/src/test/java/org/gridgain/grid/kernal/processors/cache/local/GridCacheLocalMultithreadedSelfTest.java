/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Multithreaded local cache locking test.
 */
public class GridCacheLocalMultithreadedSelfTest extends GridCommonAbstractTest {
    /** Cache. */
    private GridCache<Integer, String> cache;

    /**
     * Start grid by default.
     */
    public GridCacheLocalMultithreadedSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite ignite = grid();

        cache = ignite.cache(null);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cache = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If test fails.
     */
    public void testBasicLocks() throws Throwable {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert cache.lock(1, 1000L);

                info("Locked key from thread: " + thread());

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                cache.unlock(1);

                info("Unlocked key from thread: " + thread());

                return null;
            }
        }, 10, "basic-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLocks() throws Throwable {
        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                Collection<Integer> keys = new ArrayList<>();

                Collections.addAll(keys, 1, 2, 3);

                assert cache.lockAll(keys, 0);

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread: " + thread());

                cache.unlockAll(keys);

                info("Unlocked keys from thread: " + thread());

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSlidingKeysLocks() throws Throwable {
        final AtomicInteger cnt = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                Collection<Integer> keys = new ArrayList<>();

                Collections.addAll(keys, idx, idx + 1, idx + 2, idx + 3);

                assert cache.lockAll(keys, 0);

                info("Locked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                Thread.sleep(50);

                info("Unlocking key from thread [keys=" + keys + ", thread=" + thread() + ']');

                cache.unlockAll(keys);

                info("Unlocked keys from thread [keys=" + keys + ", thread=" + thread() + ']');

                return null;
            }
        }, 10, "multi-lock-thread");
    }

    /**
     * @throws Exception If test fails.
     */
    public void testSingleLockTimeout() throws Exception {
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert !cache.isLocked(1);

                assert cache.lock(1, 0);

                assert cache.isLockedByThread(1);
                assert cache.isLocked(1);

                l1.countDown();

                l2.await();

                cache.unlock(1);

                assert !cache.isLockedByThread(1);
                assert !cache.isLocked(1);

                return null;
            }
        }, "lock-timeout-1");

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                l1.await();

                assert cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                assert !cache.lock(1, 100L);

                assert cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                l2.countDown();

                info("Checked lockedByThread.");

                return null;
            }
        }, "lock-timeout-2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);
    }

    /**
     * @throws Exception If test fails.
     */
    public void testMultiLockTimeout() throws Exception {
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        final AtomicInteger cnt = new AtomicInteger();

        final Collection<Integer> keys1 = new ArrayList<>();
        final Collection<Integer> keys2 = new ArrayList<>();

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                assert !cache.isLocked(1);

                Collections.addAll(keys1, idx, idx + 1, idx + 2, idx + 3);

                assert cache.lockAll(keys1, 0);

                for (Integer key : keys1) {
                    assert cache.isLocked(key) : "Failed to acquire lock for key: " + key;
                    assert cache.isLockedByThread(key) : "Failed to acquire lock for key: " + key;
                }

                l1.countDown();

                l2.await();

                cache.unlockAll(keys1);

                for (Integer key : keys1) {
                    assert !cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                l3.countDown();

                return null;
            }
        }, "lock-timeout-1");

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                int idx = cnt.incrementAndGet();

                Collections.addAll(keys2, idx, idx + 1, idx + 2, idx + 3);

                l1.await();

                for (Integer key : keys1) {
                    assert cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                // Lock won't be acquired due to timeout.
                assert !cache.lockAll(keys2, 100);

                for (Integer key : keys2) {
                    boolean locked = cache.isLocked(key);

                    assert locked == keys1.contains(key) : "Lock failure for key [key=" + key +
                        ", locked=" + locked + ", keys1=" + keys1 + ']';

                    assert !cache.isLockedByThread(key);
                }

                l2.countDown();

                l3.await();

                for (Integer key : keys2) {
                    assert !cache.isLocked(key);
                    assert !cache.isLockedByThread(key);
                }

                return null;
            }
        }, "lock-timeout-2");

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        for (Integer key : keys1) {
            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }

        for (Integer key : keys2) {
            assert !cache.isLocked(key);
            assert !cache.isLockedByThread(key);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockOrder() throws Exception {
        final CountDownLatch l1 = new CountDownLatch(1);
        final CountDownLatch l2 = new CountDownLatch(1);
        final CountDownLatch l3 = new CountDownLatch(1);

        Thread t1 = new GridTestThread(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                assert cache.lock(1, 0L);

                l1.countDown();

                assert cache.isLocked(1);
                assert cache.isLockedByThread(1);

                l2.await();

                cache.unlock(1);

                l3.countDown();

                assert !cache.isLockedByThread(1);

                return null;
            }
        });

        Thread t2 = new Thread(new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                try {
                    l1.await();

                    IgniteFuture<Boolean> f1 = cache.lockAsync(1, 0L);

                    try {
                        f1.get(100, TimeUnit.MILLISECONDS);

                        assert false;
                    }
                    catch (IgniteFutureTimeoutException e) {
                        info("Correctly received timeout exception: " + e);
                    }

                    IgniteFuture<Boolean> f2 = cache.lockAsync(2, 0L);

                    try {
                        // Can't acquire f2 because f1 is held.
                        f2.get(100, TimeUnit.MILLISECONDS);

                        // TODO uncomment after GG-3756 fix
                        //assert false;
                    }
                    catch (IgniteFutureTimeoutException e) {
                        info("Correctly received timeout exception: " + e);
                    }

                    assert cache.isLocked(1);
                    assert !cache.isLockedByThread(1);

                    // TODO uncomment after GG-3756 fix
                    //assert cache.isLocked(2);
                    //assert !cache.isLockedByThread(2);

                    l2.countDown();

                    l3.await();

                    assert f1.get();

                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    assert f2.get();

                    assert cache.isLocked(2);
                    assert cache.isLockedByThread(2);

                    cache.unlock(1);
                    cache.unlock(2);

                    assert !cache.isLocked(1);
                    assert !cache.isLockedByThread(1);

                    assert !cache.isLocked(2);
                    assert !cache.isLockedByThread(2);
                }
                catch (Throwable e) {
                    error("Failed to acquire lock in thread: " + thread(), e);

                    fail("Failed to acquire lock in thread: " + thread());
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);

        assert !cache.isLocked(2);
        assert !cache.isLockedByThread(2);
    }

    /**
     * @return Formatted string for current thread.
     */
    private String thread() {
        return "Thread [id=" + Thread.currentThread().getId() + ", name=" + Thread.currentThread().getName() + ']';
    }
}
